"""DAG 60 - manual reconstruction Track A -> Track B contract dry-run.

Runs the solver, the real dbt feature SQL in an isolated DuckDB database,
Gates A-F, the CustomerState handoff, and the rule-based future generator.
It intentionally does not write production MinIO, Redis, Kafka, or Postgres.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from lzd_utils.callbacks import DEFAULT_ARGS


@dag(
    dag_id="60_reconstruction_e2e",
    description="Track A + Track B end-to-end contract dry-run",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    default_args={**DEFAULT_ARGS, "retries": 0},
    tags=["reconstruction", "track-a", "track-b", "lzd"],
    doc_md=__doc__,
)
def reconstruction_e2e():
    @task
    def run_contract() -> dict:
        import json

        from lzd_pipeline.reconstruction.e2e import run_demo

        result = run_demo()
        summary = result.summary()
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        if not result.all_passed:
            raise RuntimeError(f"reconstruction gates failed: {summary['gates']}")
        return summary

    run_contract()


reconstruction_e2e()
