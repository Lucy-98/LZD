"""Ghi so cai vao Postgres (schema ops.*).

Muc dich: moi buoc cua pipeline deu de lai dau vet tra cuu duoc bang SQL,
Grafana doc thang tu day (datasource Pipeline-Postgres).
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from lzd_pipeline.common.clients import pg_cursor
from lzd_pipeline.common.logging_setup import get_logger

log = get_logger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ===========================================================================
# ops.pipeline_run
# ===========================================================================
def start_run(
    run_id: str,
    dag_id: str,
    task_id: str,
    logical_date: date | str,
    stage: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    with pg_cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.pipeline_run
                (run_id, dag_id, task_id, logical_date, stage, status, started_at, metadata)
            VALUES (%s, %s, %s, %s, %s, 'RUNNING', %s, %s)
            ON CONFLICT (run_id) DO UPDATE
                SET status = 'RUNNING', started_at = EXCLUDED.started_at,
                    finished_at = NULL, error_message = NULL,
                    metadata = EXCLUDED.metadata
            """,
            (run_id, dag_id, task_id, logical_date, stage, _utcnow(),
             json.dumps(metadata or {})),
        )


def finish_run(
    run_id: str,
    status: str = "SUCCESS",
    rows_in: int | None = None,
    rows_out: int | None = None,
    error_message: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    with pg_cursor() as cur:
        cur.execute(
            """
            UPDATE ops.pipeline_run
               SET status = %s,
                   finished_at = %s,
                   rows_in = COALESCE(%s, rows_in),
                   rows_out = COALESCE(%s, rows_out),
                   error_message = %s,
                   metadata = metadata || %s::jsonb
             WHERE run_id = %s
            """,
            (status, _utcnow(), rows_in, rows_out, error_message,
             json.dumps(metadata or {}), run_id),
        )


# ===========================================================================
# ops.feature_sync_audit / ops.feature_sync_shard
# ===========================================================================
def open_sync(
    feature_version: str,
    logical_date: date | str,
    source_table: str,
    total_shards: int,
    expected_rows: int | None = None,
) -> None:
    """Mo (hoac mo lai) 1 lan sync. Rerun cung version se reset trang thai
    nhung GIU LAI shard da DONE -> day la co so cua resume/idempotency."""
    with pg_cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.feature_sync_audit
                (feature_version, logical_date, source_table, status,
                 total_shards, expected_rows, started_at)
            VALUES (%s, %s, %s, 'IN_PROGRESS', %s, %s, %s)
            ON CONFLICT (feature_version) DO UPDATE
                SET status = 'IN_PROGRESS',
                    total_shards = EXCLUDED.total_shards,
                    expected_rows = EXCLUDED.expected_rows,
                    started_at = EXCLUDED.started_at,
                    finished_at = NULL,
                    error_message = NULL
            """,
            (feature_version, logical_date, source_table, total_shards,
             expected_rows, _utcnow()),
        )
        # Tao san dong cho tung shard (PENDING) neu chua co
        cur.executemany(
            """
            INSERT INTO ops.feature_sync_shard (feature_version, shard_id, status)
            VALUES (%s, %s, 'PENDING')
            ON CONFLICT (feature_version, shard_id) DO NOTHING
            """,
            [(feature_version, i) for i in range(total_shards)],
        )


def record_shard(
    feature_version: str,
    shard_id: int,
    status: str,
    rows_written: int = 0,
    duration_ms: int | None = None,
    error_message: str | None = None,
    preserve_rows: bool = False,
) -> None:
    with pg_cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.feature_sync_shard
                (feature_version, shard_id, status, rows_written, attempt, duration_ms, error_message, updated_at)
            VALUES (%s, %s, %s, %s, 1, %s, %s, %s)
            ON CONFLICT (feature_version, shard_id) DO UPDATE
                SET status = EXCLUDED.status,
                    rows_written = CASE
                        WHEN %s THEN ops.feature_sync_shard.rows_written
                        ELSE EXCLUDED.rows_written
                    END,
                    attempt = ops.feature_sync_shard.attempt + 1,
                    duration_ms = EXCLUDED.duration_ms,
                    error_message = EXCLUDED.error_message,
                    updated_at = EXCLUDED.updated_at
            """,
            (feature_version, shard_id, status, rows_written, duration_ms,
             error_message, _utcnow(), preserve_rows),
        )
        cur.execute(
            """
            UPDATE ops.feature_sync_audit a
               SET completed_shards = s.done, written_rows = s.rows
              FROM (SELECT COUNT(*) FILTER (WHERE status = 'DONE') AS done,
                           COALESCE(SUM(rows_written), 0)          AS rows
                      FROM ops.feature_sync_shard
                     WHERE feature_version = %s) s
             WHERE a.feature_version = %s
            """,
            (feature_version, feature_version),
        )


def get_pending_shards(feature_version: str) -> list[int]:
    """Shard chua DONE -> chi lam lai dung nhung shard nay khi rerun."""
    with pg_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT shard_id FROM ops.feature_sync_shard
             WHERE feature_version = %s AND status <> 'DONE'
             ORDER BY shard_id
            """,
            (feature_version,),
        )
        return [row[0] for row in cur.fetchall()]


def set_sync_status(
    feature_version: str,
    status: str,
    checksum: str | None = None,
    validation_report: dict[str, Any] | None = None,
    error_message: str | None = None,
    activated: bool = False,
    written_rows: int | None = None,
) -> None:
    with pg_cursor() as cur:
        cur.execute(
            """
            UPDATE ops.feature_sync_audit
               SET status = %s,
                   checksum = COALESCE(%s, checksum),
                   validation_report = COALESCE(%s::jsonb, validation_report),
                   written_rows = COALESCE(%s, written_rows),
                   error_message = %s,
                   activated_at = CASE WHEN %s THEN %s ELSE activated_at END,
                   finished_at  = CASE WHEN %s IN ('ACTIVE','FAILED','ROLLED_BACK')
                                       THEN %s ELSE finished_at END
             WHERE feature_version = %s
            """,
            (status, checksum,
             json.dumps(validation_report) if validation_report is not None else None,
             written_rows, error_message, activated, _utcnow(), status, _utcnow(),
             feature_version),
        )


def retire_versions(keep_versions: list[str]) -> list[str]:
    """Danh dau RETIRED cho cac version khong con giu -> DAG se GC key tren Redis."""
    with pg_cursor() as cur:
        cur.execute(
            """
            UPDATE ops.feature_sync_audit
               SET status = 'RETIRED'
             WHERE status IN ('ACTIVE', 'VALIDATING')
               AND feature_version <> ALL(%s)
            RETURNING feature_version
            """,
            (keep_versions,),
        )
        return [row[0] for row in cur.fetchall()]


def get_active_version() -> str | None:
    with pg_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT feature_version FROM ops.feature_sync_audit
             WHERE status = 'ACTIVE' ORDER BY activated_at DESC LIMIT 1
            """
        )
        row = cur.fetchone()
        return row[0] if row else None


# ===========================================================================
# ops.dq_result
# ===========================================================================
def record_dq(
    logical_date: date | str,
    check_name: str,
    target: str,
    passed: bool,
    observed: float | None = None,
    threshold: float | None = None,
    severity: str = "ERROR",
    details: dict[str, Any] | None = None,
) -> None:
    with pg_cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.dq_result
                (logical_date, check_name, target, severity, passed, observed, threshold, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (logical_date, check_name, target, severity, passed, observed,
             threshold, json.dumps(details or {})),
        )
    log.info(
        "dq check",
        extra={"event": "dq_check", "check_name": check_name, "target": target,
               "passed": passed, "observed": observed, "threshold": threshold},
    )


# ===========================================================================
# ops.inference_log
# ===========================================================================
def log_inference(
    user_id: str,
    feature_version: str | None,
    model_version: str | None,
    uplift_score: float | None,
    decision: str,
    latency_ms: float,
    cache_hit: bool,
    features_missing: int = 0,
) -> None:
    """Ghi log inference (best-effort, khong duoc lam hong request)."""
    try:
        with pg_cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.inference_log
                    (user_id, feature_version, model_version, uplift_score,
                     decision, latency_ms, cache_hit, features_missing)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (user_id, feature_version, model_version, uplift_score, decision,
                 latency_ms, cache_hit, features_missing),
            )
    except Exception as exc:
        log.warning("khong ghi duoc inference_log",
                    extra={"event": "inference_log_failed", "error": str(exc)})
