"""DAG 10 - INGEST: giam sat du lieu stream do stream-consumer ghi vao lake.

stream-consumer chay lien tuc, DAG nay chay moi gio de:
  1. kiem tra co file parquet moi khong (khong co = luong streaming dut)
  2. do do tre giua event_ts va luc file duoc ghi
  3. gop file nho (compaction) - nhieu file 2000 dong lam DuckDB doc cham
  4. ghi so lieu ra Grafana + ops.pipeline_run

Day la vai tro "canh cong" giua streaming va batch.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from lzd_utils.callbacks import DEFAULT_ARGS

DOC = __doc__


@dag(
    dag_id="10_ingest_stream_to_lake",
    description="Kiem tra + compact du lieu stream trong data lake (hang gio)",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ingest", "streaming", "lzd"],
    doc_md=DOC,
)
def ingest_stream_to_lake():

    @task
    def check_new_files(**context) -> dict:
        """Dem file parquet moi trong gio vua roi."""
        from lzd_pipeline.common.clients import get_s3_client
        from lzd_pipeline.common.config import get_settings
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.common.metrics import push_batch_metrics

        log = get_logger(__name__)
        settings = get_settings()
        s3 = get_s3_client()
        window_start = context["data_interval_start"]

        prefix = f"raw/app_events/dt={window_start:%Y-%m-%d}/hour={window_start:%H}/"
        resp = s3.list_objects_v2(Bucket=settings.minio.bucket_lake, Prefix=prefix)
        objects = resp.get("Contents", [])
        total_bytes = sum(o["Size"] for o in objects)

        result = {"prefix": prefix, "files": len(objects), "bytes": total_bytes}
        log.info("kiem tra file moi", extra={"event": "lake_scan", **result})
        push_batch_metrics(
            "ingest_stream",
            {"lake_new_files": len(objects), "lake_new_bytes": total_bytes},
            labels={"dataset": "app_events"},
        )
        return result

    @task
    def measure_ingestion_lag(scan: dict) -> dict:
        """Do do tre event_ts -> ingested_at tren du lieu vua vao."""
        from lzd_pipeline.common.clients import duckdb_conn
        from lzd_pipeline.common.config import get_settings
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.common.metrics import push_batch_metrics

        log = get_logger(__name__)
        if scan["files"] == 0:
            log.warning("khong co file moi trong gio nay",
                        extra={"event": "no_new_files", "prefix": scan["prefix"]})
            return {"files": 0, "p95_lag_sec": None, "rows": 0}

        path = f"{get_settings().lake_root}/{scan['prefix']}*.parquet"
        with duckdb_conn(read_only=True) as con:
            rows, p50, p95, max_lag = con.execute(
                f"""
                SELECT COUNT(*),
                       quantile_cont(ingested_at - event_ts, 0.5),
                       quantile_cont(ingested_at - event_ts, 0.95),
                       MAX(ingested_at - event_ts)
                FROM read_parquet('{path}')
                """
            ).fetchone()

        result = {"files": scan["files"], "rows": rows,
                  "p50_lag_sec": float(p50 or 0), "p95_lag_sec": float(p95 or 0),
                  "max_lag_sec": float(max_lag or 0)}
        log.info("do tre ingest", extra={"event": "ingest_lag", **result})
        push_batch_metrics(
            "ingest_stream",
            {"ingest_lag_p50_seconds": result["p50_lag_sec"],
             "ingest_lag_p95_seconds": result["p95_lag_sec"],
             "ingest_rows": rows},
            labels={"dataset": "app_events"},
        )
        return result

    @task(pool="duckdb_writer")
    def compact_hour(scan: dict, **context) -> dict:
        """Gop nhieu file nho thanh 1 file/gio.

        Idempotent: ghi ra file `compacted.parquet` co dinh theo gio, chay lai
        se ghi de chinh no chu khong sinh them file moi.
        """
        from lzd_pipeline.common.clients import duckdb_writer, get_s3_client
        from lzd_pipeline.common.config import get_settings
        from lzd_pipeline.common.logging_setup import get_logger

        log = get_logger(__name__)
        if scan["files"] <= 1:
            log.info("khong can compact", extra={"event": "compact_skipped",
                                                 "files": scan["files"]})
            return {"compacted": False, "files": scan["files"]}

        settings = get_settings()
        window_start = context["data_interval_start"]
        src = f"{settings.lake_root}/{scan['prefix']}part-*.parquet"
        dst = f"{settings.lake_root}/curated/app_events/dt={window_start:%Y-%m-%d}/hour={window_start:%H}/compacted.parquet"

        with duckdb_writer() as con:      # pool=duckdb_writer dam bao doc quyen
            con.execute(
                f"""
                COPY (
                    SELECT * FROM read_parquet('{src}')
                    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingested_at) = 1
                ) TO '{dst}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                """
            )
            rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{dst}')").fetchone()[0]

        log.info("compact xong", extra={"event": "compacted", "target": dst,
                                        "rows": rows, "source_files": scan["files"]})
        return {"compacted": True, "target": dst, "rows": rows}

    @task
    def dq_streaming_freshness(lag: dict, **context) -> None:
        """Ghi ket qua DQ: du lieu stream co con chay khong."""
        from lzd_pipeline.common import audit

        dt = context["logical_date"].date().isoformat()
        has_data = lag["files"] > 0
        audit.record_dq(dt, "stream_has_new_files", "lake:app_events", has_data,
                        observed=float(lag["files"]), threshold=1.0,
                        severity="ERROR" if not has_data else "INFO")
        if lag.get("p95_lag_sec") is not None:
            audit.record_dq(dt, "stream_ingest_lag_p95", "lake:app_events",
                            lag["p95_lag_sec"] < 300,
                            observed=lag["p95_lag_sec"], threshold=300.0,
                            severity="WARN")

    scan = check_new_files()
    lag = measure_ingestion_lag(scan)
    compact_hour(scan) >> dq_streaming_freshness(lag)


ingest_stream_to_lake()
