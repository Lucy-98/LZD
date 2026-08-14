"""Shell `biz.*` trong DuckDB — dung cau truc, va RONG mot cach ON AO.

Muc tieu cua shell: `dbt run --select tag:reconstruction` khong con
CatalogException. Rui ro cua shell: mart chay xanh nhung RONG, va ai do doc
"dbt xanh" thanh "Track A da chay". File nay kiem ca hai mat.
"""
from __future__ import annotations

import duckdb
import pytest

from lzd_pipeline.reconstruction.feature_set import load_feature_set
from lzd_pipeline.reconstruction.warehouse import (
    BIZ_RELATIONS,
    BIZ_SCHEMA,
    ReconstructionSourcesEmpty,
    assert_reconstruction_sources_ready,
    create_biz_shell,
    reconstruction_source_counts,
)


@pytest.fixture()
def con():
    with duckdb.connect(":memory:") as c:
        yield c


# ===========================================================================
# Shell dung cau truc
# ===========================================================================
def test_tao_du_5_relation(con):
    created = create_biz_shell(con)
    assert len(created) == len(BIZ_RELATIONS) == 5
    for name in BIZ_RELATIONS:
        con.execute(f"SELECT * FROM {BIZ_SCHEMA}.{name} LIMIT 0")   # khong duoc raise


def test_idempotent(con):
    a = create_biz_shell(con)
    b = create_biz_shell(con)
    assert a == b


def test_passthrough_lay_cot_tu_ARTIFACT_khong_chep_tay(con):
    """DDL Postgres da tung troi lai scope 36 vi chep tay danh sach T3.

    Shell nay dan xuat tu `fs.tiers["T3"]` nen doi scope la no doi theo — khong
    co duong nao de lech.
    """
    create_biz_shell(con)
    cols = [r[0] for r in con.execute(
        f"DESCRIBE {BIZ_SCHEMA}.passthrough_source").fetchall()]
    fs = load_feature_set()
    assert cols[0] == "target_id"
    assert set(cols[1:]) == set(fs.tiers["T3"])
    assert len(cols) == len(fs.tiers["T3"]) + 1


def test_shell_khop_cot_ma_feat_passthrough_select(con):
    """`feat_passthrough.sql` select tung cot T3 theo ten — thieu la loi runtime."""
    from pathlib import Path

    create_biz_shell(con)
    have = {r[0] for r in con.execute(
        f"DESCRIBE {BIZ_SCHEMA}.passthrough_source").fetchall()}
    sql = (Path(__file__).resolve().parents[2]
           / "dbt" / "models" / "marts" / "feat_passthrough.sql").read_text(encoding="utf-8")
    for col in load_feature_set().tiers["T3"]:
        assert f"'{col}'" in sql          # model that su select cot nay
        assert col in have                # va shell co no


# ===========================================================================
# Rong mot cach ON AO
# ===========================================================================
def test_shell_moi_dung_la_RONG(con):
    create_biz_shell(con)
    assert set(reconstruction_source_counts(con).values()) == {0}


def test_relation_thieu_thi_bao_ro_la_thieu(con):
    counts = reconstruction_source_counts(con)          # chua tao shell
    assert set(counts.values()) == {-1}
    with pytest.raises(ReconstructionSourcesEmpty, match="thieu relation"):
        assert_reconstruction_sources_ready(con)


def test_shell_rong_thi_KHONG_duoc_coi_la_san_sang(con):
    """★ Bat bien quan trong nhat cua file nay.

    Neu ham nay pass tren shell rong, "dbt run xanh" se bi doc thanh "Track A
    da chay" — dung loi nhan thuc ma TECH_REFERENCE §12.1 canh bao.
    """
    create_biz_shell(con)
    with pytest.raises(ReconstructionSourcesEmpty, match="RONG"):
        assert_reconstruction_sources_ready(con)


def test_co_du_lieu_thi_pass(con):
    create_biz_shell(con)
    fs = load_feature_set()
    con.execute(f"INSERT INTO {BIZ_SCHEMA}.reconstruction_boundary VALUES ('t1','c1',now())")
    con.execute(f"INSERT INTO {BIZ_SCHEMA}.customer_attribute VALUES ('t1','synthetic_attr_64',0)")
    con.execute(f"INSERT INTO {BIZ_SCHEMA}.encoding_map VALUES ('e','synthetic_attr_64',0,'f37',0.1)")
    con.execute(f"INSERT INTO {BIZ_SCHEMA}.onehot_layout VALUES ('e','synthetic_segment_g1',0,'f40')")
    placeholders = ",".join(["?"] * (len(fs.tiers["T3"]) + 1))
    con.execute(
        f"INSERT INTO {BIZ_SCHEMA}.passthrough_source VALUES ({placeholders})",
        ["t1", *[0.0] * len(fs.tiers["T3"])],
    )
    assert set(assert_reconstruction_sources_ready(con)) == set(BIZ_RELATIONS)


# ===========================================================================
# Bootstrap KHONG duoc tu coi minh la that bai khi shell rong
# ===========================================================================
def test_dag_00_khong_assert_san_sang():
    """dag_00 co chu dich ket thuc o trang thai shell-rong.

    Neu no GOI `assert_reconstruction_sources_ready()` thi bootstrap se luon do
    o lan chay dau tien — dung luc chua the co du lieu.

    Kiem bang AST chu khong bang `in`: docstring cua dag_00 co NHAC ten ham do
    de giai thich, va nhac khong phai goi.
    """
    import ast
    from pathlib import Path

    src = (Path(__file__).resolve().parents[2]
           / "airflow" / "dags" / "dag_00_bootstrap_lake.py").read_text(encoding="utf-8")
    tree = ast.parse(src)

    called = {
        node.func.id if isinstance(node.func, ast.Name) else node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, (ast.Name, ast.Attribute))
    }
    assert "create_biz_shell" in called
    assert "assert_reconstruction_sources_ready" not in called
