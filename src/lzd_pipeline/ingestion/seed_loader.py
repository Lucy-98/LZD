"""Nap dataset goc (CSV) vao data lake - mo phong bang nguon tren BigQuery.

Trong production: bang nay do he thong khac ghi (batch job cua team khac) va
pipeline cua minh chi doc. O local, ta nap 1 lan tu data/full_trainset.csv.

Dac diem dataset (DESCN - Lazada voucher distribution):
    data_id, label, is_treat, f0..f82

Ta gan them:
    user_id   : dan tu data_id, tien to phu thuoc split
                    train_123 -> U0000123   (trung khong gian voi stream event
                                             => feature batch + realtime gap nhau)
                    test_123  -> T0000123
                🚫 KHONG duoc bo tien to train_/test_ khi sinh ID: hai file CSV
                   danh so doc lap tu 0 nen se va cham va lam mat du lieu train.
                   Xem giai thich day du trong `seed_split()`.
    split     : train | test
    dt        : ngay logic (partition)
    feature_ts: moc thoi gian snapshot (dung do freshness)

Chay boi DAG 00_bootstrap_lake (task nay chi can chay 1 lan).
"""
from __future__ import annotations

import os
from typing import Any

from lzd_pipeline.common.clients import duckdb_conn, duckdb_writer
from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import get_logger

log = get_logger(__name__)

DEFAULT_CSV_DIR = "/opt/project/data"


def _lake_uri(*parts: str) -> str:
    return "/".join([get_settings().lake_root.rstrip("/"), *parts])


def load_csv_to_lake(
    csv_path: str,
    split: str,
    dt: str,
    row_limit: int | None = None,
) -> dict[str, Any]:
    """Doc CSV -> ghi parquet (snappy) vao s3://lakehouse/raw/user_snapshot/.

    Dung DuckDB COPY nen khong nap het file vao RAM (file train ~476MB).
    `row_limit` de chay thu nhanh khi dev.
    """
    target = _lake_uri("raw", "user_snapshot", f"dt={dt}", f"{split}.parquet")
    limit_sql = f"LIMIT {row_limit}" if row_limit else ""

    sql = f"""
    COPY (
        SELECT
            -- data_id -> user_id, TIEN TO PHU THUOC SPLIT:
            --     train_123  ->  U0000123
            --     test_123   ->  T0000123
            --
            -- ★ `[MEASURED]` Truoc day cong thuc la
            --       'U' || lpad(regexp_extract(data_id, '\\d+'), 7, '0')
            --   tuc chi lay CHU SO va vut bo tien to. Hai file CSV danh so
            --   DOC LAP tu 0, nen `train_0` va `test_0` cung ra `U0000000` —
            --   hai nguoi khac nhau, mot ID.
            --
            --   `stg_user_snapshot` khu trung theo `(user_id, dt)` giu ban
            --   `feature_ts` moi nhat. test.parquet ghi sau nen thang, va
            --   181,669 dong TRAIN bi xoa am tham:
            --
            --       381,669 dong nap vao  ->  200,000 user_id
            --       train con lai 18,331 / 200,000  (mat 91.7%)
            --
            --   Model train tren phan sot lai cho qini 0.000128, gan nhu
            --   khong hoc duoc gi. Khong co canh bao nao: DAG xanh, dbt
            --   xanh, chi co con so cuoi cung la sai.
            --
            -- ★ Train giu tien to 'U' vi `event_producer` sinh `U{{idx:07d}}`
            --   — trung khong gian LA CO Y, de realtime overlay join duoc voi
            --   feature batch. Test khong can join voi stream (no chi dung de
            --   danh gia) nen doi sang 'T' la an toan.
            CASE WHEN lower(data_id) LIKE 'test%' THEN 'T' ELSE 'U' END
                || lpad(regexp_extract(data_id, '\\d+'), 7, '0') AS user_id,
            data_id,
            '{split}'            AS split,
            DATE '{dt}'          AS dt,
            now()                AS feature_ts,
            CAST(label    AS INTEGER) AS label,
            CAST(is_treat AS INTEGER) AS is_treat,
            * EXCLUDE (data_id, label, is_treat)
        FROM read_csv('{csv_path}', header=true, auto_detect=true, sample_size=200000)
        {limit_sql}
    ) TO '{target}' (FORMAT PARQUET, COMPRESSION SNAPPY);
    """

    # COPY ... TO 's3://' khong sua file .duckdb nhung van can session ghi
    # de DuckDB tao temp table trung gian -> dung duckdb_writer cho ro y do.
    with duckdb_writer() as con:
        log.info("bat dau nap CSV vao lake",
                 extra={"event": "seed_start", "csv": csv_path, "target": target, "split": split})
        con.execute(sql)
        rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{target}')").fetchone()[0]

    log.info("nap xong",
             extra={"event": "seed_done", "target": target, "rows": rows, "split": split})
    return {"target": target, "rows": rows, "split": split, "dt": dt}


def load_all(dt: str, csv_dir: str | None = None, row_limit: int | None = None) -> dict[str, Any]:
    csv_dir = csv_dir or os.environ.get("SEED_CSV_DIR", DEFAULT_CSV_DIR)
    results = []
    for split, filename in (("train", "full_trainset.csv"), ("test", "full_testset.csv")):
        path = os.path.join(csv_dir, filename)
        if not os.path.exists(path):
            log.warning("khong tim thay file CSV, bo qua",
                        extra={"event": "seed_missing", "path": path})
            continue
        results.append(load_csv_to_lake(path, split, dt, row_limit))
    total = sum(r["rows"] for r in results)
    return {"dt": dt, "parts": results, "total_rows": total}


if __name__ == "__main__":
    import sys

    dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-05"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
    print(load_all(dt, row_limit=limit))
