"""DAG 20 - TRANSFORM: RAW -> CLEANED -> BUSINESS READY bang dbt.

    stg_user_snapshot ─┐
    stg_app_events ────┼─> feat_user_behaviour ─┐
                       │                        ├─> feat_user_serving  (=> Redis)
                       └─> feat_user_realtime_pit ┘        │
                                                  └────────┴─> training_dataset

Sau khi dbt build xong:
  - kiem tra cot cua feat_user_serving co khop feature_spec.yml khong
    (day la hang rao chong training/serving skew, fail som con hon sai am tham)
  - day so lieu (row count, null rate, freshness) len Grafana
  - ghi ket qua dbt test vao ops.dq_result

Tat ca task ghi DuckDB deu nam trong pool `duckdb_writer` (1 slot) vi DuckDB
chi cho 1 writer tai 1 thoi diem.

⚠️ DAG nay CHI build duong feature PRODUCTION (`--exclude tag:reconstruction`).
   Model cua duong reconstruction build rieng bang `--select tag:reconstruction`
   sau khi Track A da land du lieu. Ly do: xem khoi chu thich o `DBT_SELECTOR`.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from lzd_utils.callbacks import DEFAULT_ARGS

DOC = __doc__

DBT_DIR = "/opt/project/dbt"
DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_DIR,
    "DUCKDB_PATH": "{{ var.value.get('duckdb_path', '/opt/lakehouse/warehouse.duckdb') }}",
}


@dag(
    dag_id="20_build_features_dbt",
    description="dbt: build cleaned + business-ready feature tables",
    schedule="0 1 * * *",                       # 01:00 hang ngay
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "run_date": Param(
            "1970-01-01",
            type="string",
            description="Ngay partition dt can build. Dat 1970-01-01 de build toan bo du lieu co san trong lakehouse.",
        ),
    },
)
def build_features_dbt():

    # ------------------------------------------------------------------
    # dbt deps/run/test chay bang Bash de log cua dbt hien nguyen ven
    # trong Airflow UI (va tu do chay vao Loki).
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # `--connection` chu KHONG phai `dbt debug` tran.
    #
    # `dbt debug` tran kiem ca "required dependencies", trong do co `git`.
    # Image Airflow khong cai git, nen check do do:
    #     - git [ERROR]
    #     1 check failed: Error from git --help: User does not have
    #     permissions for this command: "git"
    # va BashOperator thay exit code khac 0 -> dbt_debug fail -> ca DAG dung
    # o buoc dau, du profiles.yml, adapter va ket noi DuckDB/MinIO deu OK.
    #
    # git chi can cho `dbt deps` keo package tu git. Repo nay KHONG co
    # `dbt/packages.yml`, tuc la khong bao gio chay `dbt deps` — cai git vao
    # image chi de lam vui long mot check khong lien quan la them ~50 MB cho
    # thu khong ai dung.
    #
    # `--connection` kiem dung phan co y nghia: profiles.yml, dbt_project.yml,
    # adapter, va ket noi that toi warehouse.
    # ------------------------------------------------------------------
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_DIR} && dbt debug --connection --no-version-check",
        env=DBT_ENV,
        append_env=True,
        pool="duckdb_writer",
    )

    # ------------------------------------------------------------------
    # 🚫 `--exclude tag:reconstruction`
    #
    # Model reconstruction (`stg_events_v2`, `feat_cfs_*`, `feat_passthrough`)
    # doc `source('raw','events_v2')` va `source('biz', ...)`. Nhung relation do
    # CHI ton tai sau khi Track A chay va writer land du lieu — hien tai CHUA CO
    # writer production (xem TECH_REFERENCE.md §12.1).
    #
    # Khong exclude thi `dbt run` do CatalogException va keo sap ca duong
    # feature production hang ngay, du duong do khong lien quan gi toi
    # reconstruction. Danh sach tag nam o dbt_project.yml.
    # ------------------------------------------------------------------
    DBT_SELECTOR = "--exclude tag:reconstruction"

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && dbt run --no-version-check {DBT_SELECTOR} "
            "--vars '{\"run_date\": \"{{ params.run_date }}\"}'"
        ),
        env=DBT_ENV,
        append_env=True,
        pool="duckdb_writer",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && dbt test --no-version-check {DBT_SELECTOR} "
            "--vars '{\"run_date\": \"{{ params.run_date }}\"}'"
        ),
        env=DBT_ENV,
        append_env=True,
        pool="duckdb_writer",
        # Test fail thi bao dong nhung khong chan sync (severity xu ly o DQ DAG)
        retries=0,
    )

    @task(pool="duckdb_writer")
    def assert_spec_contract() -> dict:
        """HANG RAO CHONG SKEW: cot cua mart phai phu het spec.

        Thieu cot -> fail ngay tai day, khong de sync ghi len Redis mot bo
        feature khong khop voi luc train.
        """
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.features.offline_store import OfflineFeatureStore
        from lzd_pipeline.features.spec import load_feature_spec

        log = get_logger(__name__)
        spec = load_feature_spec()
        offline = OfflineFeatureStore(spec=spec)

        serving_cols = offline.columns(spec.offline["serving_table"])
        missing_serving = spec.validate_columns(serving_cols, scope="batch")

        if missing_serving:
            log.error(
                "mart feature THIEU COT so voi spec",
                extra={
                    "event": "spec_validation_failed",
                    "table": spec.offline["serving_table"],
                    "missing_columns": missing_serving,
                },
            )
            raise AssertionError(
                f"{spec.offline['serving_table']} thieu {len(missing_serving)} cot: "
                f"{missing_serving[:10]}..."
            )

        log.info("mart phu hop feature_spec.yml",
                 extra={"event": "spec_validated", "columns": len(serving_cols)})
        return {"columns": len(serving_cols), "status": "ok"}

    # ------------------------------------------------------------------
    # Ket qua dbt test -> Postgres ops.dq_result + Pushgateway
    #
    # Nhiem vu nay doc truc tiep `target/run_results.json` ma dbt de lai,
    # boc thanh tung ban ghi DQ, va ghi vao Postgres.
    # ------------------------------------------------------------------
    @task(pool="duckdb_writer", trigger_rule=TriggerRule.ALL_DONE)
    def publish_dbt_results(**context) -> dict:
        """Doc dbt/target/run_results.json -> ops.dq_result + Pushgateway.

        Chay ca khi `dbt test` FAIL (trigger_rule=ALL_DONE) - vi day chinh la
        luc can biet test nao hong. Neu khong co task nay, ket qua dbt test chi
        nam trong log Airflow, khong len duoc Grafana.
        """
        import json
        import os

        from lzd_pipeline.common import audit
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.common.metrics import push_batch_metrics

        log = get_logger(__name__)
        path = os.path.join(DBT_DIR, "target", "run_results.json")
        dt = context["ds"]

        if not os.path.exists(path):
            log.warning("khong tim thay run_results.json",
                        extra={"event": "dbt_results_missing", "path": path})
            return {"parsed": 0}

        with open(path, encoding="utf-8") as fh:
            payload = json.load(fh)

        counts = {"pass": 0, "fail": 0, "warn": 0, "error": 0, "skipped": 0}
        failures = []

        for result in payload.get("results", []):
            node = result.get("unique_id", "")
            status = str(result.get("status", "")).lower()
            counts[status] = counts.get(status, 0) + 1

            if not node.startswith("test."):
                continue

            passed = status in ("pass", "success")
            # so dong vi pham (dbt tra ve trong `failures`)
            observed = float(result.get("failures") or 0)

            audit.record_dq(
                dt,
                check_name=node.split(".")[-2] if node.count(".") >= 2 else node,
                target=node,
                passed=passed,
                observed=observed,
                threshold=0.0,
                severity="WARN" if status == "warn" else "ERROR",
                details={
                    "dbt_status": status,
                    "execution_time_sec": round(float(result.get("execution_time") or 0), 3),
                    "message": str(result.get("message") or "")[:500],
                },
            )
            if not passed:
                failures.append({"test": node, "status": status,
                                 "failures": observed,
                                 "message": str(result.get("message") or "")[:200]})

        push_batch_metrics(
            "build_features",
            {"dbt_tests_passed": counts.get("pass", 0),
             "dbt_tests_failed": counts.get("fail", 0) + counts.get("error", 0),
             "dbt_tests_warned": counts.get("warn", 0)},
            labels={"dag_id": "20_build_features_dbt"},
        )

        summary = {"counts": counts, "failed_tests": failures[:20]}
        (log.error if failures else log.info)(
            "ket qua dbt test",
            extra={"event": "dbt_test_results", **summary},
        )

        # Bang chung cu the nam trong schema `dq_failures` cua DuckDB
        # (store_failures: true trong dbt_project.yml):
        #   SELECT * FROM dq_failures.<ten_test> LIMIT 20;
        return summary

    @task(pool="duckdb_writer", trigger_rule=TriggerRule.ALL_DONE)
    def profile_features(**context) -> dict:
        """Do row count / null rate / freshness -> Grafana + ops.dq_result."""
        from lzd_pipeline.common import audit
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.common.metrics import push_batch_metrics
        from lzd_pipeline.features.offline_store import OfflineFeatureStore
        from lzd_pipeline.features.spec import load_feature_spec

        log = get_logger(__name__)
        spec = load_feature_spec()
        offline = OfflineFeatureStore(spec=spec)
        dt_param = context.get("params", {}).get("run_date", "1970-01-01")
        dt = dt_param if dt_param != "1970-01-01" else context["ds"]
        quality = spec.quality

        rows = offline.count_rows(dt=dt)
        if rows == 0:
            rows = offline.count_rows(dt=None)
        freshness = offline.freshness_hours()
        null_rates = offline.null_rates(spec.batch_names[:20], dt=dt if rows > 0 else None)
        worst_feature = max(null_rates, key=null_rates.get) if null_rates else None
        worst_null = null_rates.get(worst_feature, 0.0) if worst_feature else 0.0
        treatment_ratio = offline.treatment_ratio()

        audit.record_dq(dt, "mart_row_count", spec.offline["serving_table"],
                        rows >= quality.get("min_row_count", 1000),
                        observed=float(rows),
                        threshold=float(quality.get("min_row_count", 1000)))
        audit.record_dq(dt, "mart_null_rate", spec.offline["serving_table"],
                        worst_null <= quality.get("max_null_rate", 0.02),
                        observed=worst_null,
                        threshold=float(quality.get("max_null_rate", 0.02)),
                        details={"worst_feature": worst_feature})
        if freshness is not None:
            audit.record_dq(dt, "mart_freshness", spec.offline["serving_table"],
                            freshness <= quality.get("max_freshness_hours", 26),
                            observed=freshness,
                            threshold=float(quality.get("max_freshness_hours", 26)))

        metrics = {"mart_rows": rows}
        if freshness is not None:
            metrics["table_freshness_hours"] = freshness
        if treatment_ratio is not None:
            metrics["training_treatment_ratio"] = treatment_ratio
        push_batch_metrics("build_features",
                           metrics,
                           labels={"table": spec.offline["serving_table"]})
        for feature, rate in null_rates.items():
            push_batch_metrics("build_features",
                               {"feature_null_rate": rate},
                               labels={"feature": feature})

        result = {"rows": rows, "freshness_hours": freshness,
                  "worst_null_feature": worst_feature, "worst_null_rate": worst_null,
                  "treatment_ratio": treatment_ratio}
        log.info("profile mart", extra={"event": "mart_profiled", **result})
        return result

    # profile_features va publish_dbt_results deu la ALL_DONE: du dbt_test co
    # fail thi so lieu van len duoc Grafana - do la luc can nhin nhat.
    contract = assert_spec_contract()
    dbt_debug >> dbt_run >> contract >> dbt_test >> publish_dbt_results() >> profile_features()


build_features_dbt()
