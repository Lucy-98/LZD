"""DAG 99 - OPS TOOLBOX: cong cu van hanh + dien tap su co (trigger tay).

Chon `action` khi trigger DAG:

  inspect              - in trang thai feature store (khong doi gi)
  rollback             - tro active_version ve ban truoc (khac phuc su co tuc thi)
  resync_pending       - chi ghi lai nhung shard chua DONE
  chaos_partial_fail   - GIA LAP sync hong giua chung (xoa dau vai shard)
  chaos_flush_realtime - xoa toan bo overlay realtime (test cache miss)
  gc_versions          - don version cu ngay lap tuc

Dien tap khuyen nghi cho intern:
  1. Chay DAG 40 cho xong -> xem Grafana dashboard 01
  2. Chay 99 voi action=chaos_partial_fail -> shard chuyen FAILED
  3. Chay lai DAG 40 -> quan sat: chi shard hong duoc ghi lai, cac shard khac
     bi bo qua (log "shard da xong tu truoc") -> hieu idempotency
  4. Chay 99 voi action=rollback -> xem active_version doi trong 1 giay
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models import Param

from lzd_utils.callbacks import DEFAULT_ARGS

DOC = __doc__


@dag(
    dag_id="99_ops_toolbox",
    description="Cong cu van hanh + dien tap su co cho feature store",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    default_args={**DEFAULT_ARGS, "retries": 0},
    tags=["ops", "chaos", "lzd"],
    doc_md=DOC,
    params={
        "action": Param(
            "inspect",
            type="string",
            enum=["inspect", "rollback", "resync_pending", "chaos_partial_fail",
                  "chaos_flush_realtime", "gc_versions"],
        ),
        "target_date": Param("", type="string",
                             description="Ngay logic (YYYY-MM-DD). Trong = dung ngay chay DAG."),
        "target_version": Param("", type="string",
                                description="Chi dinh version khi rollback. Trong = ban truoc do."),
        "fail_shards": Param(3, type="integer", description="So shard bi pha khi chaos"),
    },
)
def ops_toolbox():

    @task
    def run_action(**context) -> dict:
        import json

        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.features import sync
        from lzd_pipeline.features.online_store import OnlineFeatureStore

        log = get_logger(__name__)
        params = context["params"]
        action = params["action"]
        target_date = params["target_date"] or context["ds"]
        store = OnlineFeatureStore()

        log.info("chay cong cu van hanh",
                 extra={"event": "ops_action", "action": action, "target_date": target_date})

        if action == "inspect":
            version = store.get_active_version()
            result = {
                "active_version": version,
                "all_versions": store.list_versions(),
                "status": store.get_version_status(version) if version else {},
                "age_seconds": store.feature_store_age_seconds(),
                "batch_keys": store.count_keys(store.spec.batch_key(version, "*")) if version else 0,
                "realtime_keys": store.count_keys(store.spec.realtime_key("*")),
                "shards_done": store.r.scard(store.spec.meta_key(version, "shards")) if version else 0,
            }

        elif action == "rollback":
            result = sync.rollback(params["target_version"] or None)

        elif action == "resync_pending":
            result = sync.sync_pending_shards(target_date)

        elif action == "chaos_partial_fail":
            victims = sync.simulate_partial_failure(target_date, int(params["fail_shards"]))
            result = {
                "broken_shards": victims,
                "huong_dan": "Chay lai DAG 40_sync_features_to_redis va quan sat "
                             "chi nhung shard nay duoc ghi lai.",
            }

        elif action == "chaos_flush_realtime":
            pattern = store.spec.realtime_key("*")
            deleted = 0
            cursor = 0
            while True:
                cursor, keys = store.r.scan(cursor=cursor, match=pattern, count=1000)
                if keys:
                    store.r.unlink(*keys)
                    deleted += len(keys)
                if cursor == 0:
                    break
            result = {"deleted_realtime_keys": deleted,
                      "huong_dan": "Xem cache miss tang tren dashboard 00, sau vai phut "
                                   "stream-consumer se dung lai overlay."}

        elif action == "gc_versions":
            result = sync.cleanup_old_versions()

        else:
            raise ValueError(f"action khong ho tro: {action}")

        print(json.dumps(result, indent=2, default=str))
        log.info("ket qua", extra={"event": "ops_action_done", "action": action,
                                   "result": json.dumps(result, default=str)[:1000]})
        return result

    run_action()


ops_toolbox()
