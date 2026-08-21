"""Duong reconstruction phai duoc TACH khoi `dbt run` mac dinh — va tach DUNG.

Boi canh (TECH_REFERENCE.md §12.1): model reconstruction doc
`source('raw','events_v2')` va `source('biz', ...)`. Nhung relation do chi ton
tai sau khi Track A land du lieu. Truoc khi co tag, `dag_20` chay `dbt run`
khong selector => build ca chung => CatalogException => sap ca duong feature
production hang ngay, du no khong lien quan gi toi reconstruction.

★ File nay kiem BA dieu, va dieu thu ba moi la thu chong drift that su:

   1. dag_20 co `--exclude tag:reconstruction`
   2. dbt_project.yml co tag cho cac model do
   3. TAP MODEL DUOC TAG == TAP MODEL THUC SU DUNG SOURCE RECONSTRUCTION

Neu chi kiem (1)(2) thi them mot model reconstruction thu 6 ma quen tag se lam
dag_20 do lai, va test van xanh. (3) doc thang tu SQL nen khong the quen.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DBT_MODELS = ROOT / "dbt" / "models"
DBT_PROJECT = ROOT / "dbt" / "dbt_project.yml"
DAG_20 = ROOT / "airflow" / "dags" / "dag_20_build_features_dbt.py"

RECONSTRUCTION_TAG = "reconstruction"

#: Source chi ton tai o duong reconstruction. Dung `source('raw','user_snapshot')`
#: hay `source('raw','app_events')` la duong production binh thuong.
_RECON_SOURCE = re.compile(
    r"source\(\s*['\"]biz['\"]|source\(\s*['\"]raw['\"]\s*,\s*['\"]events_v2['\"]"
)


def _sql_models() -> dict[str, str]:
    return {p.stem: p.read_text(encoding="utf-8") for p in DBT_MODELS.rglob("*.sql")}


def _models_using_reconstruction_sources() -> set[str]:
    """Model doc TRUC TIEP source cua duong reconstruction."""
    return {name for name, body in _sql_models().items() if _RECON_SOURCE.search(body)}


def _tagged_models() -> set[str]:
    """Model duoc gan tag `reconstruction` trong dbt_project.yml."""
    raw = yaml.safe_load(DBT_PROJECT.read_text(encoding="utf-8"))
    tagged: set[str] = set()

    def walk(node: object) -> None:
        if not isinstance(node, dict):
            return
        for key, value in node.items():
            if not isinstance(value, dict):
                continue
            tags = value.get("+tags") or []
            if RECONSTRUCTION_TAG in (tags if isinstance(tags, list) else [tags]):
                tagged.add(str(key))
            walk(value)

    walk(raw["models"]["lzd_uplift"])
    return tagged


# ===========================================================================
# 1 · DAG 20 selector
# ===========================================================================
def test_dag_20_exclude_duong_reconstruction():
    body = DAG_20.read_text(encoding="utf-8")
    assert body.count("DBT_SELECTOR") >= 3   # 1 dinh nghia + 2 lan dung


def test_dag_20_fail_fast_khi_track_a_chua_land():
    body = DAG_20.read_text(encoding="utf-8")
    assert "def validate_reconstruction_inputs()" in body
    assert "assert_reconstruction_sources_ready(con)" in body
    assert "raw/events_v2/**/*.parquet" in body
    assert "inputs_ready >> dbt_debug >> dbt_run" in body


# ===========================================================================
# 2 · dbt_project.yml phai co tag
# ===========================================================================
def test_dbt_project_gan_tag_reconstruction():
    assert _tagged_models(), "khong model nao duoc gan tag reconstruction"


# ===========================================================================
# 3 · ★ Tap tag == tap model that su dung source reconstruction
# ===========================================================================
def test_tag_phu_dung_tap_model_dung_source_reconstruction():
    """Chong drift hai chieu.

    thieu tag  => dag_20 do lai (dung loi cu)
    thua tag   => model production bi bo qua am tham, feature thieu khi sync
    """
    using = _models_using_reconstruction_sources()
    tagged = _tagged_models()

    assert using, "fixture sai: phai co it nhat mot model dung source reconstruction"
    assert tagged == using, (
        f"tag lech voi SQL.\n"
        f"  thieu tag (se lam dag_20 do): {sorted(using - tagged)}\n"
        f"  thua tag (bi bo qua oan):     {sorted(tagged - using)}"
    )


def test_model_production_khong_bi_gan_tag():
    """Duong feature hang ngay phai KHONG nam trong tag reconstruction."""
    tagged = _tagged_models()
    for name in ("feat_user_serving", "feat_user_selected_serving",
                 "feat_user_behaviour", "training_dataset", "stg_user_snapshot"):
        assert name not in tagged, f"{name} la model production, khong duoc tag"


def test_model_reconstruction_van_duoc_khai_bao_day_du():
    """Chan viec 'sua' drift bang cach xoa model khoi dbt_project.yml."""
    assert _models_using_reconstruction_sources() >= {
        "stg_events_v2", "feat_cfs_counter", "feat_cfs_recency",
        "feat_cfs_categorical", "feat_passthrough",
    }
