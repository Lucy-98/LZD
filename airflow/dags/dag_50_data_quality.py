"""DAG 50 - DATA QUALITY & FRESHNESS (chay moi 30 phut).

Kiem tra 4 nhom:
  1. FRESHNESS  - feature tren Redis co qua han khong (SLA 26h)
  2. CONSISTENCY- sample lai offline vs online (khong chi kiem tra luc sync)
  3. VOLUME     - so key Redis so voi so dong mart
  4. STREAMING  - stream con chay khong, overlay realtime con moi khong

Ket qua ghi vao ops.dq_result + Pushgateway -> Grafana dashboard 04 va alert
trong config/prometheus/alerts.yml.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from lzd_utils.callbacks import DEFAULT_ARGS

DOC = __doc__


@dag(
    dag_id="50_data_quality",
    description="Kiem tra freshness / consistency / volume cua feature store",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 1},
    tags=["dq", "monitoring", "lzd"],
    doc_md=DOC,
)
def data_quality():

    @task
    def check_freshness(**context) -> dict:
        """Feature online cu bao lau roi?"""
        from lzd_pipeline.common import audit
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.common.metrics import push_batch_metrics
        from lzd_pipeline.features.online_store import OnlineFeatureStore
        from lzd_pipeline.features.spec import load_feature_spec

        log = get_logger(__name__)
        store = OnlineFeatureStore()
        spec = load_feature_spec()
        max_hours = float(spec.quality.get("max_freshness_hours", 26))

        version = store.get_active_version()
        age = store.feature_store_age_seconds()
        dt = context["ds"]

        if version is None:
            audit.record_dq(dt, "online_store_has_active_version", "redis", False,
                            observed=0, threshold=1)
            log.error("chua co active_version tren Redis",
                      extra={"event": "no_active_version"})
            return {"version": None, "age_seconds": None}

        passed = age is not None and age <= max_hours * 3600
        audit.record_dq(dt, "online_store_freshness", f"redis:{version}", passed,
                        observed=float(age or 0), threshold=max_hours * 3600)
        push_batch_metrics(
            "data_quality",
            {"feature_store_age_seconds": age or 0},
            labels={"feature_version": version},
        )
        push_batch_metrics("data_quality", {"feature_store_active_version_info": 1},
                           labels={"feature_version": version})
        log.info("kiem tra freshness",
                 extra={"event": "dq_freshness", "feature_version": version,
                        "age_seconds": age, "passed": passed})
        return {"version": version, "age_seconds": age}

    @task
    def check_consistency(freshness: dict, **context) -> dict:
        """Lay ngau nhien user dang phuc vu, so gia tri Redis vs DuckDB.

        Khac voi validate luc sync: day la kiem tra LIEN TUC, bat duoc ca truong
        hop key bi sua tay, bi evict, hoac TTL het som.
        """
        from lzd_pipeline.common import audit
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.common.metrics import push_batch_metrics
        from lzd_pipeline.features.offline_store import OfflineFeatureStore
        from lzd_pipeline.features.online_store import OnlineFeatureStore
        from lzd_pipeline.features.spec import load_feature_spec

        log = get_logger(__name__)
        version = freshness.get("version")
        if not version:
            return {"skipped": True}

        spec = load_feature_spec()
        store = OnlineFeatureStore(spec=spec)
        offline = OfflineFeatureStore(spec=spec)
        tolerance = float(spec.quality.get("online_offline_tolerance", 1e-4))
        dt = context["ds"]

        sample_ids = store.sample_user_ids(version, limit=100)
        offline_rows = offline.fetch_users(sample_ids)

        compared = mismatched = missing = 0
        examples = []
        for user_id in sample_ids:
            expected = offline_rows.get(user_id)
            if expected is None:
                missing += 1
                continue
            batch_raw, _ = store.get_features(user_id, version=version)
            if not batch_raw:
                missing += 1
                continue
            for feat in spec.batch_features[:30]:      # 30 feature dau la du de phat hien
                exp = expected.get(feat.name)
                if exp is None:
                    continue
                got = feat.cast(batch_raw.get(feat.name))
                compared += 1
                if abs(float(exp) - float(got)) > max(tolerance, tolerance * abs(float(exp))):
                    mismatched += 1
                    if len(examples) < 5:
                        examples.append({"user_id": user_id, "feature": feat.name,
                                         "offline": float(exp), "online": float(got)})

        ratio = mismatched / max(compared, 1)
        passed = ratio <= tolerance and missing == 0
        report = {"sampled": len(sample_ids), "compared": compared,
                  "mismatched": mismatched, "missing": missing,
                  "mismatch_ratio": ratio, "examples": examples}

        audit.record_dq(dt, "online_offline_consistency_periodic", f"redis:{version}",
                        passed, observed=ratio, threshold=tolerance, details=report)
        push_batch_metrics("data_quality",
                           {"feature_validation_mismatch_ratio": ratio},
                           labels={"feature_version": version})
        (log.info if passed else log.error)(
            "kiem tra consistency", extra={"event": "dq_consistency", **report})
        return report

    @task
    def check_volume(freshness: dict, **context) -> dict:
        """So key tren Redis co khop so dong trong mart khong."""
        from lzd_pipeline.common import audit
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.common.metrics import push_batch_metrics
        from lzd_pipeline.features.offline_store import OfflineFeatureStore
        from lzd_pipeline.features.online_store import OnlineFeatureStore

        log = get_logger(__name__)
        version = freshness.get("version")
        if not version:
            return {"skipped": True}

        store = OnlineFeatureStore()
        offline = OfflineFeatureStore()
        online_keys = store.count_keys(store.spec.batch_key(version, "*"))
        offline_rows = offline.count_rows()
        diff_ratio = abs(online_keys - offline_rows) / max(offline_rows, 1)
        passed = diff_ratio <= 0.01
        dt = context["ds"]

        audit.record_dq(dt, "online_offline_row_count", f"redis:{version}", passed,
                        observed=float(online_keys), threshold=float(offline_rows),
                        details={"diff_ratio": diff_ratio})
        push_batch_metrics(
            "data_quality",
            {"feature_rows_synced_total": online_keys,
             "feature_versions_present": len(store.list_versions())},
            labels={"feature_version": version},
        )
        result = {"online_keys": online_keys, "offline_rows": offline_rows,
                  "diff_ratio": diff_ratio, "passed": passed}
        log.info("kiem tra volume", extra={"event": "dq_volume", **result})
        return result

    @task
    def check_streaming(**context) -> dict:
        """Overlay realtime con duoc cap nhat khong."""
        import time

        from lzd_pipeline.common import audit
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.common.metrics import push_batch_metrics
        from lzd_pipeline.features.online_store import OnlineFeatureStore

        log = get_logger(__name__)
        store = OnlineFeatureStore()
        dt = context["ds"]

        rt_keys = store.count_keys(store.spec.realtime_key("*"))
        # Lay 1 key bat ky, xem rt_last_event_ts cach day bao lau
        cursor, keys = store.r.scan(0, match=store.spec.realtime_key("*"), count=20)
        newest = 0.0
        for key in keys[:20]:
            ts = store.r.hget(key, "rt_last_event_ts")
            if ts:
                newest = max(newest, float(ts))
        overlay_age = time.time() - newest if newest else None

        passed = rt_keys > 0 and (overlay_age is None or overlay_age < 900)
        audit.record_dq(dt, "realtime_overlay_active", "redis:rt", passed,
                        observed=float(rt_keys), threshold=1.0, severity="WARN")
        push_batch_metrics("data_quality",
                           {"realtime_overlay_keys": rt_keys,
                            "realtime_overlay_age_seconds": overlay_age or -1},
                           labels={})
        result = {"realtime_keys": rt_keys, "overlay_age_seconds": overlay_age,
                  "passed": passed}
        log.info("kiem tra streaming", extra={"event": "dq_streaming", **result})
        return result

    @task
    def check_compatibility(freshness: dict, **context) -> dict:
        """Block a green DQ result when the serving tuple is incompatible."""
        from lzd_pipeline.common import audit
        from lzd_pipeline.features.compatibility import check_model_feature_compatibility
        from lzd_pipeline.features.online_store import OnlineFeatureStore
        from lzd_pipeline.features.spec import load_feature_spec
        from lzd_pipeline.serving.model_loader import get_model

        version = freshness.get("version")
        if not version:
            return {"compatible": False, "reasons": ["no active feature version"]}
        store = OnlineFeatureStore()
        spec = load_feature_spec()
        model = get_model(force_reload=True)
        if not model.is_configured:
            result = {"compatible": False, "reasons": ["model is not configured"]}
        else:
            report = check_model_feature_compatibility(
                model_features=model.feature_order,
                model_feature_spec_version=model.feature_spec_version,
                model_realtime_semantics_version=model.realtime_semantics_version,
                requires_realtime=model.requires_realtime,
                spec=spec,
                active_status=store.get_version_status(version),
            )
            result = report.as_dict()
        audit.record_dq(
            context["ds"], "model_feature_compatibility", f"redis:{version}",
            bool(result["compatible"]), observed=float(bool(result["compatible"])),
            threshold=1.0, details=result,
        )
        return result

    @task
    def summarize(
        consistency: dict, volume: dict, streaming: dict, compatibility: dict
    ) -> dict:
        """Gom ket qua - task nay fail neu co check nghiem trong that bai."""
        from lzd_pipeline.common.logging_setup import get_logger

        log = get_logger(__name__)
        failures = []
        if not consistency.get("skipped") and consistency.get("mismatched", 0) > 0:
            failures.append(f"consistency: {consistency['mismatched']} gia tri lech")
        if not volume.get("skipped") and not volume.get("passed", True):
            failures.append(
                f"volume: redis={volume.get('online_keys')} vs duckdb={volume.get('offline_rows')}")
        if not compatibility.get("compatible", False):
            failures.append(f"compatibility: {compatibility.get('reasons')}")

        result = {"failures": failures, "streaming_ok": streaming.get("passed")}
        if failures:
            log.error("DQ that bai", extra={"event": "dq_summary_failed", **result})
            raise ValueError("Data quality that bai: " + "; ".join(failures))
        log.info("DQ pass het", extra={"event": "dq_summary_ok", **result})
        return result

    fresh = check_freshness()
    summarize(
        check_consistency(fresh), check_volume(fresh), check_streaming(),
        check_compatibility(fresh),
    )


data_quality()
