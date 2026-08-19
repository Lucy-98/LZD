"""Factory cho cac client ha tang: Redis, DuckDB, MinIO (S3), Postgres.

Tap trung o 1 file de:
  - retry/timeout dong nhat
  - DuckDB luon duoc cau hinh san extension httpfs + credential MinIO
  - de mock trong unit test
"""
from __future__ import annotations

import contextlib
import os
from typing import Any, Iterator

from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------- Redis
def get_redis(decode_responses: bool = True):
    """Client Redis dung chung.

    decode_responses=True -> tra ve str (tien cho feature dang so/text).
    socket_timeout thap vi serving path yeu cau < 10ms; that bai nhanh con hon treo.
    """
    import redis

    cfg = get_settings().redis
    return redis.Redis(
        host=cfg.host,
        port=cfg.port,
        db=cfg.db,
        decode_responses=decode_responses,
        socket_timeout=2.0,
        socket_connect_timeout=2.0,
        health_check_interval=30,
        retry_on_timeout=True,
        max_connections=64,
    )


# --------------------------------------------------------------- DuckDB
class DuckDBWriteLockError(RuntimeError):
    """Khong lay duoc quyen ghi vi process khac dang giu."""


def get_duckdb(read_only: bool = True, path: str | None = None):
    """Ket noi DuckDB (dong vai tro BigQuery trong ban local).

    MAC DINH LA READ-ONLY - CO Y NHU VAY.

    DuckDB la embedded single-writer: mot process mo file o che do ghi se
    KHOA toan bo file. Neu mac dinh la ghi thi chi can mot notebook cua data
    scientist go `duckdb_conn()` de query linh tinh la dbt dang chay luc 01:00
    se chet giua chung, de lai bang mart do dang.

    Muon ghi thi phai noi ro `read_only=False`, va task do BAT BUOC chay trong
    pool `duckdb_writer` (1 slot) cua Airflow.
    """
    import duckdb

    settings = get_settings()
    db_path = path or settings.duckdb_path

    if not read_only:
        # Ghi lai ai dang giu khoa - de khi co su co con truy ra thu pham
        # trong Loki: {job="docker"} | json | event="duckdb_write_open"
        log.info(
            "mo DuckDB o che do GHI (khoa toan file)",
            extra={"event": "duckdb_write_open", "path": db_path,
                   "service": get_settings().service_name,
                   "pid": os.getpid()},
        )

    try:
        con = duckdb.connect(db_path, read_only=read_only)
    except Exception as exc:
        if "lock" in str(exc).lower():
            raise DuckDBWriteLockError(
                f"Khong mo duoc {db_path} (read_only={read_only}): {exc}\n"
                "Nguyen nhan thuong gap: mot process khac dang mo file o che do GHI.\n"
                "  - Kiem tra task nao dang chay trong pool `duckdb_writer`\n"
                "  - Query ad-hoc PHAI dung duckdb_conn() (mac dinh read-only)\n"
                "  - Xem ai mo write gan day: Loki -> event=\"duckdb_write_open\""
            ) from exc
        raise

    minio = settings.minio
    try:
        con.execute("LOAD httpfs;")
    except Exception:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception as httpfs_err:
            log.warning("Khong load duoc httpfs extension trong DuckDB: %s", httpfs_err)

    try:
        con.execute(f"SET s3_endpoint='{minio.host_no_scheme}';")
        con.execute(f"SET s3_access_key_id='{minio.access_key}';")
        con.execute(f"SET s3_secret_access_key='{minio.secret_key}';")
        con.execute(f"SET s3_use_ssl={'true' if minio.use_ssl else 'false'};")
        con.execute("SET s3_url_style='path';")   # bat buoc voi MinIO
        con.execute("SET enable_progress_bar=false;")
    except Exception as s3_cfg_err:
        log.debug("Khong set duoc S3 settings cho DuckDB: %s", s3_cfg_err)
    return con


@contextlib.contextmanager
def duckdb_conn(read_only: bool = True) -> Iterator[Any]:
    """Mac dinh READ-ONLY. Xem giai thich o get_duckdb()."""
    con = get_duckdb(read_only=read_only)
    try:
        yield con
    finally:
        con.close()


@contextlib.contextmanager
def duckdb_writer() -> Iterator[Any]:
    """Mo DuckDB de GHI - viet ro rang de doc code thay ngay.

    Chi dung trong task Airflow thuoc pool `duckdb_writer`.
    """
    con = get_duckdb(read_only=False)
    try:
        yield con
    finally:
        con.close()


# ---------------------------------------------------------------- MinIO
def get_s3_client():
    """boto3 client tro vao MinIO."""
    import boto3
    from botocore.client import Config

    cfg = get_settings().minio
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        config=Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"}),
        region_name="us-east-1",
    )


# -------------------------------------------------------------- Postgres
def get_pg_connection():
    """Ket noi Postgres (db `pipeline`) cho bang audit ops.*"""
    import psycopg2

    return psycopg2.connect(get_settings().postgres.dsn)


@contextlib.contextmanager
def pg_cursor(commit: bool = True) -> Iterator[Any]:
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ----------------------------------------------------------------- Kafka
def get_kafka_producer(**overrides: Any):
    from confluent_kafka import Producer

    cfg = get_settings().kafka
    conf = {
        "bootstrap.servers": cfg.bootstrap_servers,
        "client.id": get_settings().service_name,
        "linger.ms": 20,
        "batch.size": 64 * 1024,
        "compression.type": "snappy",
        "acks": "all",                 # khong mat message khi broker restart
        "enable.idempotence": True,    # khong duplicate khi retry
        "retries": 5,
    }
    conf.update(overrides)
    return Producer(conf)


def get_kafka_consumer(group_id: str | None = None, **overrides: Any):
    from confluent_kafka import Consumer

    cfg = get_settings().kafka
    conf = {
        "bootstrap.servers": cfg.bootstrap_servers,
        "group.id": group_id or cfg.consumer_group,
        "auto.offset.reset": "earliest",
        # TU commit offset -> chi commit SAU khi da ghi lake roi cap nhat Redis
        # (at-least-once). Day la diem mau chot cua fault tolerance.
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
    }
    conf.update(overrides)
    return Consumer(conf)
