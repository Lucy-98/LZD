"""Land Track A xuong ha tang — kiem tren artifact THAT tu train split.

Ba cau hoi file nay tra loi:

  1. Artifact co dung SHAPE ma Postgres doi khong? (kiem duoc ma khong can PG)
  2. Nap vao DuckDB co dung so dong, dung cot khong?
  3. Sau khi nap, SQL dbt chay tren ten 2 phan PRODUCTION co ra dung 55 cot
     va Gate A co pass khong?

(3) la cau quan trong nhat: no di het duong tu CSV that -> event -> sink ->
dbt SQL -> Gate A, khong dung relation map phang cua harness.
"""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path

import duckdb
import pytest

from lzd_pipeline.reconstruction import runner
from lzd_pipeline.reconstruction.e2e import load_runtime_config
from lzd_pipeline.reconstruction.feature_set import load_feature_set
from lzd_pipeline.reconstruction.sink import (
    BIZ_LOAD_ORDER,
    DUCKDB_SKIP,
    POSTGRES_SKIP,
    SinkError,
    artifact_row_counts,
    load_biz_into_duckdb,
    load_events_into_duckdb,
    postgres_load_plan,
    publish_events_to_lake,
    verify_duckdb_matches_artifact,
)
from lzd_pipeline.reconstruction.track_a_batch import DEFAULT_INPUT, materialize_track_a
from lzd_pipeline.reconstruction.warehouse import BIZ_SCHEMA, create_biz_shell

ROOT = Path(__file__).resolve().parents[2]
DDL = ROOT / "sql" / "postgres" / "02_biz_reconstruction.sql"
DBT = ROOT / "dbt" / "models"

def _has_real_dataset() -> bool:
    if not DEFAULT_INPUT.exists():
        return False
    try:
        with open(DEFAULT_INPUT, "r", encoding="utf-8") as f:
            first = f.readline()
            return not first.startswith("version https://git-lfs")
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _has_real_dataset(), reason="can data/full_trainset.csv that (khong phai Git LFS pointer)"
)


@pytest.fixture(scope="module")
def artifact(tmp_path_factory) -> Path:
    """Chay Track A that tren mot lat train split nho."""
    out = tmp_path_factory.mktemp("track_a")
    materialize_track_a(
        input_path=DEFAULT_INPUT,
        output_dir=out,
        limit=200,
        config=load_runtime_config(),
        verify_limit=0,          # Gate A duoc kiem o test duoi, qua duong production
    )
    return out


# ===========================================================================
# 1 · Artifact khop hop dong Postgres — kiem duoc ma khong can Postgres
# ===========================================================================
def _ddl_columns(relation: str) -> set[str]:
    body = DDL.read_text(encoding="utf-8").split(
        f"CREATE TABLE IF NOT EXISTS {BIZ_SCHEMA}.{relation} (")[1].split("\n);")[0]
    cols = set()
    for line in body.splitlines():
        m = re.match(r"\s*(f?\w+)\s+(TEXT|INT|DOUBLE|JSONB|TIMESTAMPTZ)", line)
        if m and m.group(1).upper() not in {"PRIMARY", "CONSTRAINT", "CHECK"}:
            cols.add(m.group(1))
        for m2 in re.finditer(r"\b(f\d+)\s+DOUBLE", line):
            cols.add(m2.group(1))
    return cols


@pytest.mark.parametrize(
    "csv_name,relation",
    [(n, r) for n, r in BIZ_LOAD_ORDER if r not in POSTGRES_SKIP],
)
def test_cot_artifact_nam_tron_trong_DDL_postgres(artifact, csv_name, relation):
    """COPY se do neu artifact co cot Postgres khong biet."""
    with (artifact / csv_name).open(newline="", encoding="utf-8") as fh:
        header = set(next(csv.reader(fh)))
    unknown = header - _ddl_columns(relation)
    assert not unknown, f"{csv_name}: cot khong co trong biz.{relation}: {sorted(unknown)}"


def test_boundary_la_VIEW_nen_khong_COPY_vao_postgres():
    """`biz.reconstruction_boundary` dan xuat tu `reconstruction_target`.

    Nap `reconstruction_target` la du; COPY vao view se do.
    """
    ddl = DDL.read_text(encoding="utf-8")
    assert "CREATE OR REPLACE VIEW biz.v_reconstruction_boundary" in ddl
    assert f"CREATE TABLE IF NOT EXISTS {BIZ_SCHEMA}.reconstruction_boundary" not in ddl
    assert "reconstruction_boundary" in POSTGRES_SKIP


def test_view_boundary_expose_dung_ten_cot_ma_dbt_doc():
    """★ Bug that: view tung expose `lzd_user_id` trong khi `feat_cfs_*` select
    `customer_id_hint`. Harness khong bat duoc vi no seed bang phang cua rieng
    no — loi chi lo o PRODUCTION voi 'column does not exist'.
    """
    view = DDL.read_text(encoding="utf-8").split(
        "CREATE OR REPLACE VIEW biz.v_reconstruction_boundary AS")[1].split(";")[0]
    assert "customer_id_hint" in view

    for model in ("feat_cfs_counter", "feat_cfs_recency"):
        sql = (DBT / "marts" / f"{model}.sql").read_text(encoding="utf-8")
        if "reconstruction_boundary" not in sql:
            continue
        for col in re.findall(r"\b(customer_id_hint|lzd_user_id)\b", sql):
            assert col in view, f"{model} doc `{col}` nhung view khong expose"


def test_reconstruction_target_du_cot_bat_buoc(artifact):
    """`created_at` co DEFAULT; moi cot NOT NULL con lai phai co trong artifact."""
    with (artifact / "biz_reconstruction_target.csv").open(newline="", encoding="utf-8") as fh:
        header = set(next(csv.reader(fh)))
    assert header >= {
        "target_id", "lzd_user_id", "selected_feature_set_id", "feature_version",
        "reference_ts", "split", "payload", "target_hash", "feature_payload_hash",
    }


def test_payload_dung_scope_va_khong_co_label(artifact):
    """`ck_scope_55` + `ck_no_label` se tu choi neu sai — kiem truoc o day."""
    fs = load_feature_set()
    with (artifact / "biz_reconstruction_target.csv").open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows
    for row in rows[:20]:
        payload = json.loads(row["payload"])
        assert set(payload) == set(fs.columns)
        assert len(payload) == fs.expected_column_count == 30
        assert not {"label", "is_treat"} & set(payload)


def test_lzd_user_id_khop_quy_tac_seed_loader(artifact):
    """`train_123` -> `U0000123`, cung quy tac voi raw.user_snapshot."""
    with (artifact / "biz_reconstruction_target.csv").open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    for row in rows[:20]:
        digits = "".join(c for c in row["target_id"] if c.isdigit())
        assert row["lzd_user_id"] == "U" + digits.rjust(7, "0")


def test_thu_tu_nap_ton_trong_khoa_ngoai(artifact):
    """`customer_attribute`/`passthrough_source` co FK toi `reconstruction_target`."""
    order = [rel for _, rel in BIZ_LOAD_ORDER]
    assert order[0] == "reconstruction_target"
    for child in ("customer_attribute", "passthrough_source"):
        assert order.index(child) > order.index("reconstruction_target")


def test_plan_postgres_mo_ta_duoc_ma_khong_can_postgres(artifact):
    plan = postgres_load_plan(artifact)
    assert len(plan) == len(BIZ_LOAD_ORDER) - len(POSTGRES_SKIP)
    assert all(step["rows"] > 0 for step in plan)
    assert plan[0]["relation"] == f"{BIZ_SCHEMA}.reconstruction_target"
    assert all(f"{BIZ_SCHEMA}.{r}" not in {s["relation"] for s in plan}
               for r in POSTGRES_SKIP)


def test_hai_kho_chua_bo_qua_dung_relation():
    """Postgres bo view; DuckDB bo target. Hai tap PHAI khac nhau va roi nhau."""
    assert POSTGRES_SKIP == {"reconstruction_boundary"}
    assert DUCKDB_SKIP == {"reconstruction_target"}
    assert not POSTGRES_SKIP & DUCKDB_SKIP
    everything = {r for _, r in BIZ_LOAD_ORDER}
    assert POSTGRES_SKIP | DUCKDB_SKIP <= everything


# ===========================================================================
# 2 · Nap DuckDB
# ===========================================================================
@pytest.fixture()
def con(artifact):
    with duckdb.connect(":memory:") as c:
        c.execute("SET TimeZone='UTC'")
        for s in ("raw", "staging", "marts"):
            c.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
        create_biz_shell(c)
        yield c


def test_nap_duckdb_dung_so_dong(con, artifact):
    loaded = load_biz_into_duckdb(con, artifact)
    expected = artifact_row_counts(artifact)
    for relation, n in loaded.items():
        assert n == expected[relation], relation
    assert verify_duckdb_matches_artifact(con, artifact)


def test_khong_nap_reconstruction_target_vao_warehouse(con, artifact):
    """★ INVARIANT 4 — `payload` la 55 feature. Dua no vao warehouse la mo
    duong cho Track B nhin trom target."""
    loaded = load_biz_into_duckdb(con, artifact)
    assert "reconstruction_target" not in loaded
    with pytest.raises(duckdb.CatalogException):
        con.execute(f"SELECT * FROM {BIZ_SCHEMA}.reconstruction_target LIMIT 1")


def test_nap_lai_khong_nhan_doi(con, artifact):
    a = load_biz_into_duckdb(con, artifact)
    b = load_biz_into_duckdb(con, artifact)
    assert a == b


def test_lech_so_dong_thi_bao_loi(con, artifact):
    load_biz_into_duckdb(con, artifact)
    con.execute(f"DELETE FROM {BIZ_SCHEMA}.customer_attribute WHERE rowid IN "
                f"(SELECT rowid FROM {BIZ_SCHEMA}.customer_attribute LIMIT 1)")
    with pytest.raises(SinkError, match="lech artifact"):
        verify_duckdb_matches_artifact(con, artifact)


def test_thieu_artifact_thi_bao_ro(tmp_path, con):
    with pytest.raises(SinkError, match="thieu artifact"):
        load_biz_into_duckdb(con, tmp_path)


def test_publish_lake_ghi_parquet_dung_so_dong(con, artifact, tmp_path):
    """Dung duong file thay cho s3:// — cung code COPY, khong can MinIO."""
    target = (tmp_path / "events.parquet").as_posix()
    report = publish_events_to_lake(artifact, "2026-08-05", con=con, target_uri=target)
    assert report["rows"] == artifact_row_counts(artifact)["events"] > 0
    cols = [c[0] for c in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{target}')").fetchall()]
    assert {"event_id", "customer_id", "target_id", "event_type",
            "event_ts", "observation_ts"} <= set(cols)


# ===========================================================================
# 3 · ★ Gate A qua duong PRODUCTION-shaped
# ===========================================================================
PROD_RELATIONS = {
    ("ref", "stg_events_v2"): "staging.stg_events_v2",
    ("source", "raw", "events_v2"): "raw.events_v2",
    ("source", "biz", "reconstruction_boundary"): f"{BIZ_SCHEMA}.reconstruction_boundary",
    ("source", "biz", "customer_attribute"): f"{BIZ_SCHEMA}.customer_attribute",
    ("source", "biz", "encoding_map"): f"{BIZ_SCHEMA}.encoding_map",
    ("source", "biz", "onehot_layout"): f"{BIZ_SCHEMA}.onehot_layout",
    ("source", "biz", "passthrough_source"): f"{BIZ_SCHEMA}.passthrough_source",
}


def test_gate_a_pass_qua_duong_production_shaped(con, artifact, monkeypatch):
    """★ Bai kiem quan trong nhat cua file nay.

    CSV that -> Track A -> sink -> SQL dbt (ten 2 phan nhu production) -> Gate A.
    Khong dung relation map phang cua harness.
    """
    cfg = load_runtime_config()
    load_biz_into_duckdb(con, artifact)
    assert load_events_into_duckdb(con, artifact) > 0

    monkeypatch.setattr(runner, "RELATIONS", PROD_RELATIONS)
    for relation, path in (
        ("staging.stg_events_v2", DBT / "staging" / "stg_events_v2.sql"),
        ("marts.feat_cfs_counter", DBT / "marts" / "feat_cfs_counter.sql"),
        ("marts.feat_cfs_recency", DBT / "marts" / "feat_cfs_recency.sql"),
        ("marts.feat_cfs_categorical", DBT / "marts" / "feat_cfs_categorical.sql"),
        ("marts.feat_passthrough", DBT / "marts" / "feat_passthrough.sql"),
    ):
        sql = runner.render_model(path, {
            "f30_semantic_branch": cfg.semantic_branch,
            "history_days": 30,
            "counter_window_days": 365,
            "reconstruction_encoding_version": cfg.encoding_version,
        })
        con.execute(f"CREATE OR REPLACE TABLE {relation} AS {sql}")

    fs = load_feature_set()
    with (artifact / "targets_selected_features.csv").open(newline="", encoding="utf-8") as fh:
        targets = list(csv.DictReader(fh))
    assert targets

    checked = 0
    for row in targets[:25]:
        actual: dict[str, float] = {}
        for table in ("marts.feat_cfs_counter", "marts.feat_cfs_recency",
                      "marts.feat_cfs_categorical", "marts.feat_passthrough"):
            names = [c[0] for c in con.execute(f"DESCRIBE {table}").fetchall()]
            want = [c for c in names if c.startswith("f") and c[1:].isdigit()]
            got = con.execute(
                f"SELECT {', '.join(want)} FROM {table} WHERE target_id = ?",
                [row["target_id"]],
            ).fetchone()
            assert got is not None, f"{table} thieu {row['target_id']}"
            actual.update({c: (float(v) if v is not None else None)
                           for c, v in zip(want, got)})

        expected = {c: float(row[c]) for c in fs.columns}
        report = runner.gate_a(expected, actual, fs)
        assert report.gate_a_pass, [
            (f.column, f.regime, f.expected, f.actual) for f in report.failures()]
        assert report.gate_a_t3_pass
        checked += 1

    assert checked == min(25, len(targets))
    assert len(fs.gate_a_columns) == 9 and len(fs.gate_a_t3_columns) == 21
