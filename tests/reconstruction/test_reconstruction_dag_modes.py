"""Regression guards cho trang thai Airflow cua hai mode reconstruction."""
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DAG_60 = ROOT / "airflow" / "dags" / "dag_60_reconstruction_e2e.py"


def test_inactive_mode_is_really_skipped_in_airflow():
    body = DAG_60.read_text(encoding="utf-8")

    assert "AirflowSkipException" in body
    assert 'return {"skipped":' not in body
    assert "land=False: artifact da duoc tao" in body


def test_backfill_runs_after_contract_branch_is_skipped():
    body = DAG_60.read_text(encoding="utf-8")

    assert "trigger_rule=TriggerRule.ALL_DONE" in body
    assert "contract_dry_run() >> backfill_train_split() >> assert_sources_ready()" in body


def test_default_trigger_lands_backfill_inputs_for_dag_20():
    body = DAG_60.read_text(encoding="utf-8")

    assert '"backfill", type="string", enum=["contract", "backfill"]' in body
    assert '"land": Param(\n            True, type="boolean"' in body
