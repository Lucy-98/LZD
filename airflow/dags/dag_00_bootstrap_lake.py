"""DAG 00 - BOOTSTRAP: nap dataset goc vao data lake + dung schema DuckDB.

Chay 1 lan (trigger tay) khi dung stack lan dau.
Mo phong: bang nguon tren BigQuery da co san du lieu.

    data/full_trainset.csv  ->  s3://lakehouse/raw/user_snapshot/dt=.../train.parquet
    data/full_testset.csv   ->  s3://lakehouse/raw/user_snapshot/dt=.../test.parquet
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models import Param

from lzd_utils.callbacks import DEFAULT_ARGS

DOC = __doc__


@dag(
    dag_id="00_bootstrap_lake",
    description="Nap CSV goc vao MinIO + tao schema DuckDB (chay 1 lan)",
    schedule=None,                       # chi trigger tay
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingest", "bootstrap", "lzd"],
    doc_md=DOC,
    params={
        "dt": Param("2026-08-05", type="string",
                    description="Ngay logic gan cho snapshot"),
        "row_limit": Param(0, type="integer",
                           description="Gioi han so dong moi file (0 = lay het). "
                                       "Dat 200000 de chay thu cho nhanh."),
    },
)
def bootstrap_lake():

    @task(pool="duckdb_writer")
    def create_schemas() -> list[str]:
        """Tao schema trong DuckDB truoc khi dbt build."""
        from lzd_pipeline.common.clients import duckdb_writer
        from lzd_pipeline.common.logging_setup import get_logger

        log = get_logger(__name__)
        schemas = ["raw", "staging", "marts", "dq_failures"]
        with duckdb_writer() as con:      # pool=duckdb_writer dam bao doc quyen
            for schema in schemas:
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        log.info("tao schema xong", extra={"event": "schema_created", "schemas": schemas})
        return schemas

    @task(pool="duckdb_writer", execution_timeout=pendulum.duration(hours=1))
    def seed_csv(**context) -> dict:
        """Doc CSV bang DuckDB roi COPY thang ra parquet tren MinIO."""
        from lzd_pipeline.ingestion.seed_loader import load_all

        params = context["params"]
        limit = int(params["row_limit"]) or None
        return load_all(dt=params["dt"], row_limit=limit)

    @task
    def verify_lake(seed_result: dict) -> dict:
        """Doc lai parquet vua ghi de chac chan lake dung duoc."""
        from lzd_pipeline.common.clients import duckdb_conn
        from lzd_pipeline.common.config import get_settings
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.common.metrics import push_batch_metrics

        log = get_logger(__name__)
        lake_root = get_settings().lake_root
        path = f"{lake_root}/raw/user_snapshot/**/*.parquet"

        with duckdb_conn(read_only=True) as con:
            rows, users = con.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT user_id) FROM read_parquet('{path}')"
            ).fetchone()
            sample = con.execute(
                f"SELECT user_id, split, label, is_treat FROM read_parquet('{path}') LIMIT 3"
            ).fetchall()

        log.info("kiem tra lake",
                 extra={"event": "lake_verified", "rows": rows, "users": users,
                        "sample": str(sample)})
        push_batch_metrics("bootstrap_lake",
                           {"lake_rows": rows, "lake_users": users},
                           labels={"dataset": "user_snapshot"})

        if rows == 0:
            raise ValueError("Lake rong - kiem tra lai duong dan CSV trong ./data")
        return {"rows": rows, "users": users, **seed_result}

    @task
    def next_steps(verified: dict) -> str:
        msg = (
            f"Lake da san sang: {verified['rows']:,} dong / {verified['users']:,} user.\n"
            "Buoc tiep theo:\n"
            "  1) Bat DAG 20_build_features_dbt -> tao bang marts.feat_user_serving\n"
            "  2) Bat DAG 40_sync_features_to_redis -> day feature len Redis\n"
            "  3) Kiem tra: curl http://localhost:8000/store/info"
        )
        print(msg)
        return msg

    schemas = create_schemas()
    seeded = seed_csv()
    verified = verify_lake(seeded)
    schemas >> seeded >> verified >> next_steps(verified)


bootstrap_lake()
