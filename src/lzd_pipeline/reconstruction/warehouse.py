"""Bootstrap cac relation `biz.*` cua duong reconstruction trong DuckDB.

`dbt/models/staging/_sources.yml` khai bao:

    - name: biz
      tables: [reconstruction_boundary, customer_attribute, encoding_map,
               onehot_layout, passthrough_source]

Nhung relation nay la NGUON CUA SU THAT o Postgres (`sql/postgres/02_biz_reconstruction.sql`).
DuckDB — dong vai tro warehouse — phai co chung truoc khi chay model reconstruction.
Module nay dung phan SHELL (cau truc, khong du lieu) trong DuckDB.

★ VI SAO KHONG ATTACH POSTGRES
------------------------------------------------------------------------------
ATTACH bien moi relation thanh ten 3 phan (`pg.biz.customer_attribute`), buoc
phai sua khai bao source cua dbt va them phu thuoc vao extension `postgres`.
Materialize giu ten 2 phan `biz.x` dung nhu dbt dang khai bao, va dung dung
tu ma `_sources.yml` da chon: *"attach/materialize"*.

★ SHELL RONG KHONG PHAI LA XONG VIEC
------------------------------------------------------------------------------
Ham nay tao bang RONG. `dbt run --select tag:reconstruction` sau do se chay
duoc nhung cho ra mart RONG — do la trang thai HOP LE tam thoi, khong phai
thanh cong. `assert_reconstruction_sources_ready()` la cho de phan biet hai
truong hop, va phai duoc goi TRUOC khi ai do doc ket qua mart.

    shell dung roi    => dbt run KHONG con CatalogException
    du lieu chua co   => mart rong, Gate A vo nghia

🚫 Khong duoc doc "dbt run xanh" thanh "Track A da chay".
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from lzd_pipeline.reconstruction.feature_set import SelectedFeatureSet, load_feature_set

if TYPE_CHECKING:  # pragma: no cover
    import duckdb

#: Ten schema chua control plane cua reconstruction. Phai khop `_sources.yml`.
BIZ_SCHEMA = "biz"

#: Tat ca relation `biz.*` ma model reconstruction doc toi.
#: 🚫 Doi danh sach nay phai doi CA `_sources.yml` LAN Postgres DDL.
BIZ_RELATIONS = (
    "reconstruction_boundary",
    "customer_attribute",
    "encoding_map",
    "onehot_layout",
    "passthrough_source",
)


def _passthrough_columns(fs: SelectedFeatureSet) -> str:
    """Cot T3 lay tu ARTIFACT, khong chep tay.

    `feat_passthrough.sql` select tung cot T3 theo ten. Chep tay danh sach nay
    la dung cach ma DDL Postgres da troi lai o scope 36 — bang chi co 18 cot
    trong khi artifact da len 24.
    """
    return ", ".join(f"{col} DOUBLE" for col in fs.tiers["T3"])


def create_biz_shell(
    con: "duckdb.DuckDBPyConnection", fs: SelectedFeatureSet | None = None
) -> list[str]:
    """Tao schema `biz` + 5 bang rong dung cau truc. Idempotent.

    Tra ve ten day du cua cac relation da dung.
    """
    fs = fs or load_feature_set()
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {BIZ_SCHEMA}")

    # `reconstruction_boundary` o Postgres la VIEW `v_reconstruction_boundary`
    # (xem `_sources.yml: identifier`). Trong DuckDB no la bang phang, vi day
    # chi la ban sao doc-only cua control plane.
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {BIZ_SCHEMA}.reconstruction_boundary(
            target_id VARCHAR, customer_id_hint VARCHAR, reference_ts TIMESTAMPTZ)
    """)
    # `_sources.yml` dung identifier `v_reconstruction_boundary` de giong
    # Postgres control plane. DuckDB giu bang phang de sink nap nhanh, sau do
    # expose cung ten view cho dbt production — neu thieu view nay harness van
    # xanh nhung `dbt run --select tag:reconstruction` se CatalogException.
    con.execute(f"""
        CREATE OR REPLACE VIEW {BIZ_SCHEMA}.v_reconstruction_boundary AS
        SELECT target_id, customer_id_hint, reference_ts
        FROM {BIZ_SCHEMA}.reconstruction_boundary
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {BIZ_SCHEMA}.customer_attribute(
            target_id VARCHAR, attr_name VARCHAR, level_id INTEGER)
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {BIZ_SCHEMA}.encoding_map(
            encoding_version VARCHAR, attr_name VARCHAR, level_id INTEGER,
            column_name VARCHAR, value DOUBLE)
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {BIZ_SCHEMA}.onehot_layout(
            encoding_version VARCHAR, attr_name VARCHAR, level_index INTEGER,
            column_name VARCHAR)
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {BIZ_SCHEMA}.passthrough_source(
            target_id VARCHAR, {_passthrough_columns(fs)})
    """)
    return [f"{BIZ_SCHEMA}.{name}" for name in BIZ_RELATIONS]


def reconstruction_source_counts(
    con: "duckdb.DuckDBPyConnection",
) -> dict[str, int]:
    """So dong hien co cua tung relation `biz.*`. Thieu bang => -1."""
    counts: dict[str, int] = {}
    for name in BIZ_RELATIONS:
        try:
            row = con.execute(f"SELECT count(*) FROM {BIZ_SCHEMA}.{name}").fetchone()
            counts[name] = int(row[0]) if row else 0
        except Exception:
            counts[name] = -1
    return counts


class ReconstructionSourcesEmpty(RuntimeError):
    """Shell co nhung chua co du lieu — mart reconstruction se rong."""


def assert_reconstruction_sources_ready(con: "duckdb.DuckDBPyConnection") -> dict[str, int]:
    """Chan viec doc mart reconstruction rong nhu the no co y nghia.

    🚫 KHONG goi ham nay trong `dag_00`. Bootstrap CO CHU DICH ket thuc o trang
       thai shell-rong; do la trang thai dung cua no. Ham nay danh cho buoc DOC
       ket qua reconstruction — noi mart rong la mot loi im lang.
    """
    counts = reconstruction_source_counts(con)
    missing = [k for k, v in counts.items() if v < 0]
    if missing:
        raise ReconstructionSourcesEmpty(
            f"thieu relation {missing} — chay `create_biz_shell()` truoc (dag_00)"
        )
    empty = [k for k, v in counts.items() if v == 0]
    if empty:
        raise ReconstructionSourcesEmpty(
            f"relation {empty} dang RONG. Mart reconstruction se rong va Gate A "
            "se vo nghia. Track A chua land du lieu — xem TECH_REFERENCE.md §12.1."
        )
    return counts
