"""DAG 40 - FEATURE SYNC: DuckDB (offline) -> Redis (online).  ⭐ DAG QUAN TRONG NHAT

    prepare_sync
        │  (khoa row_count + checksum cua offline, mo phien version vYYYYMMDD)
        ▼
    sync_shard[0..N-1]      <- dynamic task mapping, chay song song, retry rieng le
        │  (shard nao DONE roi thi bo qua -> rerun khong ghi trung)
        ▼
    validate_sync
        │  (so mau Redis vs DuckDB + so dong + checksum. FAIL -> dung tai day,
        │   active_version KHONG doi, serving van dung ban cu -> khong anh huong user)
        ▼
    activate_version        <- SET fs:meta:active_version = v20260805  (ATOMIC)
        │
        ▼
    cleanup_old_versions    <- xoa version qua cu (cache invalidation)

4 tinh chat phai dat:
  CONSISTENCY  - validate truoc khi doi con tro
  FRESHNESS    - version theo ngay + DQ canh bao neu > 26h
  IDEMPOTENCY  - version deterministic + HSET + danh dau shard
  FAULT TOLER. - shard doc lap, retry tung shard, recover tu diem dung

Dien tap su co: dung DAG `99_ops_toolbox` de gia lap sync loi giua chung
roi chay lai DAG nay -> chi nhung shard hong duoc ghi lai.
"""
from __future__ import annotations

import os

import pendulum
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lzd_utils.callbacks import DEFAULT_ARGS

DOC = __doc__

# So shard phai co dinh luc parse DAG (dynamic mapping can biet truoc so task)
SHARDS = int(os.environ.get("FEATURE_SYNC_SHARDS", "32"))


@dag(
    dag_id="40_sync_features_to_redis",
    description="Sync feature tu DuckDB len Redis: versioned, idempotent, validated",
    schedule="30 1 * * *",                       # 01:30, sau khi DAG 20 xong
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 3},
    tags=["sync", "feature-store", "lzd"],
    doc_md=DOC,
    params={
        "force_full_resync": Param(
            False, type="boolean",
            description="Xoa dau shard cua version nay va ghi lai tu dau",
        ),
    },
)
def sync_features_to_redis():

    @task
    def prepare(**context) -> dict:
        """Khoa so lieu offline + mo phien sync. Fail som neu mart chua san sang."""
        from lzd_pipeline.features.online_store import OnlineFeatureStore
        from lzd_pipeline.features.sync import make_version, prepare_sync

        ds = context["ds"]
        if context["params"].get("force_full_resync"):
            version = make_version(ds)
            store = OnlineFeatureStore()
            store.r.delete(store.spec.meta_key(version, "shards"))

        return prepare_sync(ds, shards=SHARDS)

    @task(
        pool="redis_sync",
        retries=3,
        max_active_tis_per_dag=4,       # gioi han ghi song song vao Redis
        execution_timeout=pendulum.duration(minutes=20),
    )
    def sync_shard(shard_id: int, **context) -> dict:
        """Ghi 1 shard. Retry cua Airflow an toan vi ham nay idempotent."""
        from lzd_pipeline.features.sync import sync_shard as do_sync

        return do_sync(context["ds"], shard_id=shard_id, shards=SHARDS)

    @task
    def summarize_shards(results: list[dict], **context) -> dict:
        """Tong hop ket qua map -> so shard xong, so dong, shard nao bo qua."""
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.common.metrics import push_batch_metrics
        from lzd_pipeline.features.sync import make_version

        log = get_logger(__name__)
        version = make_version(context["ds"])
        total_rows = sum(r["rows"] for r in results)
        skipped = [r["shard_id"] for r in results if r.get("skipped")]

        summary = {"version": version, "shards": len(results),
                   "rows": total_rows, "skipped_shards": skipped}
        log.info("tong hop shard", extra={"event": "shards_summary", **summary})
        push_batch_metrics(
            "feature_sync",
            {"feature_sync_completed_shards": len(results),
             "feature_sync_written_rows": total_rows},
            labels={"feature_version": version},
        )
        return summary

    @task(retries=0)
    def validate(summary: dict, **context) -> dict:
        """So du lieu online vs offline. Fail -> KHONG doi active_version."""
        from lzd_pipeline.features.sync import validate_sync

        return validate_sync(context["ds"])

    @task
    def activate(report: dict, **context) -> dict:
        """Doi con tro active_version - 1 lenh SET, atomic."""
        from lzd_pipeline.features.sync import activate_version

        return activate_version(context["ds"])

    @task
    def cleanup(activated: dict) -> dict:
        """Cache invalidation: xoa version qua cu, giu N ban gan nhat."""
        from lzd_pipeline.features.sync import cleanup_old_versions

        return cleanup_old_versions()

    @task
    def smoke_test_serving(activated: dict) -> dict:
        """Doc thu vai user qua duong serving that de chac chan Redis dung duoc."""
        import time

        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.features.online_store import OnlineFeatureStore

        log = get_logger(__name__)
        store = OnlineFeatureStore()
        version = activated["version"]
        sample = store.sample_user_ids(version, limit=20)
        if not sample:
            raise ValueError(f"Khong doc duoc user nao cua version {version}")

        latencies = []
        for user_id in sample:
            started = time.perf_counter()
            merged, missing, hit = store.get_features_merged(user_id)
            latencies.append((time.perf_counter() - started) * 1000)
            if not hit:
                raise ValueError(f"Cache miss ngay sau khi sync: user_id={user_id}")

        result = {
            "version": version,
            "sampled": len(sample),
            "p50_lookup_ms": round(sorted(latencies)[len(latencies) // 2], 3),
            "max_lookup_ms": round(max(latencies), 3),
            "example_user": sample[0],
        }
        log.info("smoke test serving", extra={"event": "serving_smoke_test", **result})
        return result

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def on_sync_failed(**context) -> None:
        """Chay khi bat ky buoc nao that bai: danh dau FAILED + bao dong.

        KHONG rollback tu dong: active_version chua he bi doi nen serving van
        dang chay ban cu, an toan. Nguoi truc quyet dinh xu ly tiep.
        """
        from lzd_pipeline.features.sync import mark_failed

        mark_failed(context["ds"], f"DAG that bai o run {context['run_id']}")

    prepared = prepare()
    shard_results = sync_shard.expand(shard_id=list(range(SHARDS)))
    prepared >> shard_results

    summary = summarize_shards(shard_results)
    report = validate(summary)
    activated = activate(report)
    activated >> [cleanup(activated), smoke_test_serving(activated)]

    [prepared, shard_results, summary, report, activated] >> on_sync_failed()


sync_features_to_redis()
