"""OFFLINE FEATURE STORE (DuckDB - dong vai tro BigQuery).

Day la NGUON SU THAT. Redis chi la ban sao phuc vu doc nhanh.
Moi truy van phuc vu sync/validate/DQ deu tap trung o file nay.
"""
from __future__ import annotations

import hashlib
from typing import Any, Iterator, Sequence

from lzd_pipeline.common.clients import duckdb_conn
from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.features.spec import FeatureSpec, load_feature_spec

log = get_logger(__name__)


class OfflineFeatureStore:
    def __init__(self, spec: FeatureSpec | None = None, table: str | None = None) -> None:
        self.spec = spec or load_feature_spec()
        self.table = table or self.spec.offline["serving_table"]

    # ------------------------------------------------------------------
    # Sharding: bam user_id thanh N nhom co dinh.
    # Cung user_id -> luon cung shard => rerun 1 shard khong dung du lieu
    # cua shard khac => co the retry rieng le.
    # ------------------------------------------------------------------
    @staticmethod
    def shard_expr(entity_key: str, shards: int) -> str:
        return f"(hash({entity_key}) % {shards})"

    def count_rows(self, dt: str | None = None) -> int:
        where = f"WHERE dt = DATE '{dt}'" if dt else ""
        with duckdb_conn(read_only=True) as con:
            return con.execute(f"SELECT COUNT(*) FROM {self.table} {where}").fetchone()[0]

    def iter_shard(
        self,
        shard_id: int,
        shards: int,
        dt: str | None = None,
        chunk_size: int = 5000,
    ) -> Iterator[list[dict[str, Any]]]:
        """Doc 1 shard theo tung chunk (khong nap toan bo vao RAM)."""
        columns = [self.spec.entity_key] + self.spec.batch_names
        col_sql = ", ".join(f'"{c}"' for c in columns)
        where = [f"{self.shard_expr(self.spec.entity_key, shards)} = {shard_id}"]
        if dt:
            where.append(f"dt = DATE '{dt}'")
        sql = f"SELECT {col_sql} FROM {self.table} WHERE {' AND '.join(where)}"

        with duckdb_conn(read_only=True) as con:
            cursor = con.execute(sql)
            names = [d[0] for d in cursor.description]
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                yield [dict(zip(names, row)) for row in rows]

    def fetch_users(self, user_ids: Sequence[str], dt: str | None = None) -> dict[str, dict[str, Any]]:
        """Lay feature offline cua 1 nhom user - dung de doi chieu voi Redis."""
        if not user_ids:
            return {}
        columns = [self.spec.entity_key] + self.spec.batch_names
        col_sql = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(["?"] * len(user_ids))
        where = [f"{self.spec.entity_key} IN ({placeholders})"]
        params: list[Any] = list(user_ids)
        if dt:
            where.append("dt = ?")
            params.append(dt)
        sql = f"SELECT {col_sql} FROM {self.table} WHERE {' AND '.join(where)}"

        with duckdb_conn(read_only=True) as con:
            cursor = con.execute(sql, params)
            names = [d[0] for d in cursor.description]
            return {
                str(row[0]): dict(zip(names, row))
                for row in cursor.fetchall()
            }

    # ------------------------------------------------------------------
    # Checksum: dau van tay cua du lieu offline tai 1 thoi diem.
    # So khop checksum truoc/sau sync => biet du lieu co bi doi giua chung khong.
    # ------------------------------------------------------------------
    def checksum(self, dt: str | None = None, sample_features: int = 5) -> str:
        feats = self.spec.batch_names[:sample_features]
        agg = ", ".join([f'COALESCE(SUM("{f}"), 0)' for f in feats]) or "0"
        where = f"WHERE dt = DATE '{dt}'" if dt else ""
        with duckdb_conn(read_only=True) as con:
            row = con.execute(
                f"SELECT COUNT(*), {agg} FROM {self.table} {where}"
            ).fetchone()
        payload = "|".join(f"{v:.6f}" if isinstance(v, float) else str(v) for v in row)
        return hashlib.sha256(payload.encode()).hexdigest()[:16]

    # ------------------------------------------------------------------
    # Data quality
    # ------------------------------------------------------------------
    def freshness_hours(self, table: str | None = None) -> float | None:
        table = table or self.table
        ts_col = self.spec.offline.get("event_time_column", "feature_ts")
        with duckdb_conn(read_only=True) as con:
            try:
                row = con.execute(
                    f"SELECT date_diff('second', MAX({ts_col}), now()) / 3600.0 FROM {table}"
                ).fetchone()
            except Exception as exc:
                log.warning("khong doc duoc freshness",
                            extra={"event": "freshness_error", "table": table, "error": str(exc)})
                return None
        return float(row[0]) if row and row[0] is not None else None

    def null_rates(self, features: Sequence[str] | None = None, dt: str | None = None) -> dict[str, float]:
        features = list(features or self.spec.batch_names)
        if not features:
            return {}
        parts = [
            f'SUM(CASE WHEN "{f}" IS NULL THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) AS "{f}"'
            for f in features
        ]
        where = f"WHERE dt = DATE '{dt}'" if dt else ""
        with duckdb_conn(read_only=True) as con:
            cursor = con.execute(f"SELECT {', '.join(parts)} FROM {self.table} {where}")
            names = [d[0] for d in cursor.description]
            row = cursor.fetchone()
        return {n: float(v or 0.0) for n, v in zip(names, row)}

    def feature_means(self, features: Sequence[str], dt: str | None = None) -> dict[str, float]:
        if not features:
            return {}
        parts = [f'AVG("{f}") AS "{f}"' for f in features]
        where = f"WHERE dt = DATE '{dt}'" if dt else ""
        with duckdb_conn(read_only=True) as con:
            cursor = con.execute(f"SELECT {', '.join(parts)} FROM {self.table} {where}")
            names = [d[0] for d in cursor.description]
            row = cursor.fetchone()
        return {n: float(v or 0.0) for n, v in zip(names, row)}

    def table_exists(self, table: str | None = None) -> bool:
        table = table or self.table
        schema, _, name = table.rpartition(".")
        with duckdb_conn(read_only=True) as con:
            row = con.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_name = ? AND (? = '' OR table_schema = ?)
                """,
                [name, schema, schema],
            ).fetchone()
        return bool(row[0])

    def columns(self, table: str | None = None) -> list[str]:
        table = table or self.table
        with duckdb_conn(read_only=True) as con:
            cursor = con.execute(f"SELECT * FROM {table} LIMIT 0")
            return [d[0] for d in cursor.description]
