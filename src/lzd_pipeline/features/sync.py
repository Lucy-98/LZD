"""SYNC ENGINE: DuckDB (offline, su that) -> Redis (online, phuc vu).

4 tinh chat bat buoc cua job nay:

  1. CONSISTENCY  - gia tri tren Redis phai giong het DuckDB. Kiem chung bang
                    buoc `validate_sync` (so mau + checksum) TRUOC khi doi
                    con tro active_version.
  2. FRESHNESS    - version duoc dat ten theo ngay logic; DQ check bao dong
                    neu feature qua 26h chua refresh.
  3. IDEMPOTENCY  - version deterministic theo logical_date + ghi bang HSET +
                    danh dau shard da xong. Chay lai DAG cho ket qua y het,
                    khong nhan doi du lieu.
  4. FAULT TOL.   - chia N shard doc lap; shard nao that bai chi retry shard do;
                    du lieu chi duoc "cong bo" o buoc activate atomic cuoi cung.

So do trang thai:
    IN_PROGRESS -> VALIDATING -> ACTIVE -> RETIRED
                       |
                       +-------> FAILED (giu nguyen active cu, khong anh huong serving)
"""
from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from lzd_pipeline.common import audit
from lzd_pipeline.common.clients import duckdb_conn
from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.common.metrics import push_batch_metrics
from lzd_pipeline.features.offline_store import OfflineFeatureStore
from lzd_pipeline.features.online_store import OnlineFeatureStore
from lzd_pipeline.features.spec import load_feature_spec

log = get_logger(__name__)

PUSH_JOB = "feature_sync"


# ===========================================================================
def _parse_logical_date(logical_date: str | date | datetime) -> date:
    if isinstance(logical_date, datetime):
        return logical_date.date()
    if isinstance(logical_date, date):
        return logical_date
    if isinstance(logical_date, str):
        cleaned = logical_date.strip()
        if not cleaned:
            return date.today()
        cleaned = cleaned.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(cleaned).date()
        except ValueError:
            try:
                return datetime.strptime(cleaned[:10], "%Y-%m-%d").date()
            except ValueError:
                return date.today()
    return date.today()


def make_version(logical_date: str | date | datetime) -> str:
    """Version DETERMINISTIC theo ngay logic.

    Chay lai DAG cua ngay 2026-08-05 luon cho version 'v20260805'
    -> ghi de dung cho key cu -> khong sinh rac, khong double-write.
    """
    d = _parse_logical_date(logical_date)
    return f"v{d.strftime('%Y%m%d')}"


def dt_of(logical_date: str | date | datetime) -> str:
    d = _parse_logical_date(logical_date)
    return d.isoformat()


@dataclass
class SyncContext:
    version: str
    dt: str
    shards: int
    expected_rows: int = 0
    checksum: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


# ===========================================================================
# BUOC 1: chuan bi
# ===========================================================================
def prepare_sync(logical_date: str, shards: int | None = None) -> dict[str, Any]:
    """Khoa so lieu offline (row count + checksum) va mo phien sync."""
    settings = get_settings()
    shards = shards or settings.feature_store.shards
    version = make_version(logical_date)
    dt = dt_of(logical_date)

    offline = OfflineFeatureStore()
    online = OnlineFeatureStore()

    if not offline.table_exists():
        raise RuntimeError(
            f"Bang offline '{offline.table}' chua ton tai. Chay DAG 20_build_features_dbt truoc."
        )

    missing = load_feature_spec().validate_columns(offline.columns(), scope="batch")
    if missing:
        raise RuntimeError(
            f"Bang {offline.table} thieu cot so voi feature_spec.yml: {missing[:10]}"
            " -> sua dbt model hoac sua spec (day chinh la co che chong skew)."
        )

    expected_rows = offline.count_rows(dt=dt)
    if expected_rows == 0:
        total_rows = offline.count_rows()
        if total_rows == 0:
            raise RuntimeError(f"Bang offline '{offline.table}' chua co du lieu. Chay DAG 20_build_features_dbt truoc.")
        with duckdb_conn(read_only=True) as con:
            max_dt_val = con.execute(f"SELECT MAX(dt) FROM {offline.table}").fetchone()[0]
        if max_dt_val:
            dt = max_dt_val.isoformat()
            version = make_version(dt)
            expected_rows = offline.count_rows(dt=dt)
        if expected_rows == 0:
            raise RuntimeError(f"Khong co dong nao cho dt={dt} trong {offline.table}")

    checksum = offline.checksum(dt=dt)

    audit.open_sync(version, dt, offline.table, shards, expected_rows)
    online.set_version_status(
        version,
        status="IN_PROGRESS",
        dt=dt,
        expected_rows=expected_rows,
        checksum=checksum,
        total_shards=shards,
        started_at=int(time.time()),
    )

    log.info(
        "chuan bi sync",
        extra={"event": "sync_prepare", "feature_version": version, "dt": dt,
               "expected_rows": expected_rows, "shards": shards, "checksum": checksum},
    )
    push_batch_metrics(
        PUSH_JOB,
        {"feature_sync_total_shards": shards, "feature_sync_expected_rows": expected_rows},
        labels={"feature_version": version},
    )
    return {"version": version, "dt": dt, "shards": shards,
            "expected_rows": expected_rows, "checksum": checksum}


# ===========================================================================
# BUOC 2: ghi tung shard (chay song song duoc)
# ===========================================================================
def sync_shard(logical_date: str, shard_id: int, shards: int | None = None) -> dict[str, Any]:
    settings = get_settings()
    shards = shards or settings.feature_store.shards
    version = make_version(logical_date)
    dt = dt_of(logical_date)

    online = OnlineFeatureStore()
    offline = OfflineFeatureStore()
    slog = get_logger(__name__, feature_version=version, shard_id=shard_id)

    # Idempotency: shard da xong o lan chay truoc -> bo qua ngay
    if online.is_shard_done(version, shard_id):
        audit.record_shard(version, shard_id, "DONE", rows_written=0)
        slog.info("shard da xong tu truoc, bo qua", extra={"event": "shard_skipped"})
        return {"shard_id": shard_id, "rows": 0, "skipped": True}

    started = time.perf_counter()
    rows_written = 0
    try:
        for chunk in offline.iter_shard(shard_id, shards, dt=dt):
            rows_written += online.write_rows(version, chunk)
        elapsed_ms = int((time.perf_counter() - started) * 1000)

        online.mark_shard_done(version, shard_id, rows_written)
        audit.record_shard(version, shard_id, "DONE", rows_written, elapsed_ms)

        slog.info("ghi shard xong",
                  extra={"event": "shard_written", "rows": rows_written,
                         "duration_ms": elapsed_ms})
        return {"shard_id": shard_id, "rows": rows_written, "skipped": False,
                "duration_ms": elapsed_ms}

    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        audit.record_shard(version, shard_id, "FAILED", rows_written, elapsed_ms, str(exc))
        slog.error("shard that bai",
                   extra={"event": "shard_failed", "rows": rows_written, "error": str(exc)})
        raise


def sync_pending_shards(logical_date: str, shards: int | None = None) -> dict[str, Any]:
    """Chay lai CHI nhung shard chua DONE (dung khi recover thu cong)."""
    version = make_version(logical_date)
    pending = audit.get_pending_shards(version)
    log.info("shard con lai", extra={"event": "shard_pending", "feature_version": version,
                                     "pending": pending})
    total = 0
    for shard_id in pending:
        total += sync_shard(logical_date, shard_id, shards)["rows"]
    return {"version": version, "recovered_shards": pending, "rows": total}


# ===========================================================================
# BUOC 3: validate (offline vs online) truoc khi cong bo
# ===========================================================================
def validate_sync(logical_date: str, sample_size: int | None = None) -> dict[str, Any]:
    settings = get_settings()
    sample_size = sample_size or settings.feature_store.validation_sample
    tolerance = float(load_feature_spec().quality.get("online_offline_tolerance", 1e-4))
    version = make_version(logical_date)
    dt = dt_of(logical_date)

    online = OnlineFeatureStore()
    offline = OfflineFeatureStore()
    spec = online.spec

    online.set_version_status(version, status="VALIDATING")
    audit.set_sync_status(version, "VALIDATING")

    # --- 3.1 dem so dong ---------------------------------------------------
    expected_rows = offline.count_rows(dt=dt)
    online_rows = online.count_keys(spec.batch_key(version, "*"))
    row_diff_ratio = abs(expected_rows - online_rows) / max(expected_rows, 1)

    # --- 3.2 checksum khong doi trong luc sync -----------------------------
    checksum_now = offline.checksum(dt=dt)
    status = online.get_version_status(version)
    checksum_before = status.get("checksum", "")
    checksum_stable = (checksum_before == checksum_now) if checksum_before else True

    # --- 3.3 so gia tri tung feature tren mau ngau nhien -------------------
    sample_ids = online.sample_user_ids(version, limit=sample_size)
    offline_rows = offline.fetch_users(sample_ids, dt=dt)

    mismatched: list[dict[str, Any]] = []
    missing_keys = 0
    compared = 0

    for user_id in sample_ids:
        expected = offline_rows.get(user_id)
        if expected is None:
            missing_keys += 1
            continue
        batch_raw, _ = online.get_features(user_id, version=version)
        if not batch_raw:
            missing_keys += 1
            continue
        for feat in spec.batch_features:
            exp_val = expected.get(feat.name)
            if exp_val is None:
                continue
            got_val = feat.cast(batch_raw.get(feat.name))
            compared += 1
            if feat.dtype in ("float", "int"):
                if not _close(float(exp_val), float(got_val), tolerance):
                    mismatched.append({"user_id": user_id, "feature": feat.name,
                                       "offline": float(exp_val), "online": float(got_val)})
            elif str(exp_val) != str(got_val):
                mismatched.append({"user_id": user_id, "feature": feat.name,
                                   "offline": str(exp_val), "online": str(got_val)})

    mismatch_ratio = len(mismatched) / max(compared, 1)
    passed = (
        mismatch_ratio <= tolerance
        and missing_keys == 0
        and row_diff_ratio <= 0.001
        and checksum_stable
    )

    report = {
        "sampled": len(sample_ids),
        "compared": compared,
        "mismatched": len(mismatched),
        "mismatch_ratio": round(mismatch_ratio, 8),
        "missing_keys": missing_keys,
        "expected_rows": expected_rows,
        "online_rows": online_rows,
        "row_diff_ratio": round(row_diff_ratio, 6),
        "checksum_before": checksum_before,
        "checksum_after": checksum_now,
        "checksum_stable": checksum_stable,
        "examples": mismatched[:10],
        "passed": passed,
    }

    audit.set_sync_status(version, "VALIDATING", checksum=checksum_now, validation_report=report)
    audit.record_dq(dt, "online_offline_consistency", f"redis:{version}", passed,
                    observed=mismatch_ratio, threshold=tolerance, details=report)
    push_batch_metrics(
        PUSH_JOB,
        {"feature_validation_mismatch_ratio": mismatch_ratio,
         "feature_sync_written_rows": online_rows},
        labels={"feature_version": version},
    )

    log_fn = log.info if passed else log.error
    log_fn("ket qua validate",
           extra={"event": "sync_validated", "feature_version": version, **{
               k: v for k, v in report.items() if k != "examples"}})

    if not passed:
        raise RuntimeError(
            f"Validate that bai cho {version}: mismatch={len(mismatched)} "
            f"missing={missing_keys} row_diff={row_diff_ratio:.4f} "
            f"checksum_stable={checksum_stable}. KHONG doi active_version."
        )
    return report


def _close(a: float, b: float, tolerance: float) -> bool:
    if math.isnan(a) and math.isnan(b):
        return True
    return abs(a - b) <= max(tolerance, tolerance * abs(a))


# ===========================================================================
# BUOC 4: cong bo (atomic) + BUOC 5: don rac
# ===========================================================================
def activate_version(logical_date: str) -> dict[str, Any]:
    version = make_version(logical_date)
    online = OnlineFeatureStore()

    status = online.get_version_status(version)
    started_at = float(status.get("started_at") or time.time())
    written_rows = int(float(status.get("rows_written") or 0))

    previous = online.activate_version(version)          # <-- 1 lenh SET, atomic
    audit.set_sync_status(version, "ACTIVE", activated=True)

    duration = max(time.time() - started_at, 0.001)
    push_batch_metrics(
        PUSH_JOB,
        {
            "feature_sync_duration_seconds": duration,
            "feature_sync_write_rate_rows_per_sec": written_rows / duration,
            "feature_store_age_seconds": 0,
            "feature_rows_synced_total": written_rows,
        },
        labels={"feature_version": version},
    )
    push_batch_metrics(
        PUSH_JOB,
        {"feature_store_active_version_info": 1},
        labels={"feature_version": version},
    )

    log.info("cong bo version moi",
             extra={"event": "version_activated", "feature_version": version,
                    "previous_version": previous, "rows": written_rows,
                    "duration_sec": round(duration, 2)})
    return {"version": version, "previous": previous, "rows": written_rows}


def cleanup_old_versions(keep: int | None = None) -> dict[str, Any]:
    online = OnlineFeatureStore()
    result = online.gc_old_versions(keep=keep)
    audit.retire_versions(result["kept"])
    push_batch_metrics(
        PUSH_JOB,
        {"feature_versions_present": len(online.list_versions())},
        labels={},
    )
    log.info("don version cu", extra={"event": "version_gc", **result})
    return result


def mark_failed(logical_date: str, error: str) -> None:
    version = make_version(logical_date)
    OnlineFeatureStore().set_version_status(version, status="FAILED", error=error[:500])
    audit.set_sync_status(version, "FAILED", error_message=error[:2000])
    push_batch_metrics(PUSH_JOB, {"feature_sync_status": 1},
                       labels={"feature_version": version, "status": "FAILED"})
    log.error("sync that bai",
              extra={"event": "sync_failed", "feature_version": version, "error": error})


def rollback(target_version: str | None = None) -> dict[str, Any]:
    """Tro active_version ve version truoc do (khong can ghi lai du lieu)."""
    online = OnlineFeatureStore()
    if target_version is None:
        versions = online.list_versions()
        current = online.get_active_version()
        candidates = [v for v in versions if v != current]
        if not candidates:
            raise RuntimeError("Khong con version nao de rollback")
        target_version = candidates[0]
    online.rollback_to(target_version)
    audit.set_sync_status(target_version, "ACTIVE", activated=True)
    return {"rolled_back_to": target_version}


# ===========================================================================
def simulate_partial_failure(logical_date: str, fail_shards: int = 3) -> list[int]:
    """Cong cu day hoc: co tinh xoa dau shard da xong de dien tap recovery.

    Dung tu Airflow UI (DAG 99_chaos) hoac:
        python -c "from lzd_pipeline.features.sync import simulate_partial_failure as s; s('2026-08-05')"
    """
    version = make_version(logical_date)
    online = OnlineFeatureStore()
    all_shards = list(range(get_settings().feature_store.shards))
    victims = random.sample(all_shards, min(fail_shards, len(all_shards)))
    for shard_id in victims:
        online.r.srem(online.spec.meta_key(version, "shards"), str(shard_id))
        audit.record_shard(version, shard_id, "FAILED", 0, 0, "chaos: xoa dau shard")
    log.warning("gia lap sync loi giua chung",
                extra={"event": "chaos_partial_failure", "feature_version": version,
                       "shards": victims})
    return victims
