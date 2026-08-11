"""Callback dung chung cho moi DAG.

Mac dinh Airflow chi ghi log khi task fail. O day ta lam them:
  - ghi 1 dong vao ops.pipeline_run (Grafana doc duoc bang SQL)
  - day metric len Pushgateway (Grafana ve duoc bieu do)
  - log JSON co du dag_id/task_id/run_id (Loki loc duoc)
"""
from __future__ import annotations

from typing import Any

from lzd_pipeline.common import audit
from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.common.metrics import push_batch_metrics

log = get_logger(__name__)


def _ids(context: dict[str, Any]) -> tuple[str, str, str, str]:
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = f"{dag_id}__{ti.run_id}__{ti.task_id}__{ti.try_number}"
    logical_date = context["logical_date"].date().isoformat()
    return dag_id, task_id, run_id, logical_date


def on_task_start(context: dict[str, Any]) -> None:
    dag_id, task_id, run_id, logical_date = _ids(context)
    try:
        audit.start_run(run_id, dag_id, task_id, logical_date,
                        stage=context["dag"].tags[0] if context["dag"].tags else "unknown")
    except Exception as exc:
        log.warning("khong ghi duoc pipeline_run",
                    extra={"event": "audit_write_failed", "error": str(exc)})


def on_task_success(context: dict[str, Any]) -> None:
    dag_id, task_id, run_id, _ = _ids(context)
    try:
        audit.finish_run(run_id, "SUCCESS")
    except Exception:
        pass
    push_batch_metrics(
        dag_id,
        {"task_last_success_timestamp": context["task_instance"].end_date.timestamp()
         if context["task_instance"].end_date else 0},
        labels={"dag_id": dag_id, "task_id": task_id},
    )


def on_task_failure(context: dict[str, Any]) -> None:
    dag_id, task_id, run_id, _ = _ids(context)
    error = str(context.get("exception", ""))[:2000]
    log.error(
        "TASK THAT BAI",
        extra={"event": "task_failed", "dag_id": dag_id, "task_id": task_id,
               "run_id": context["task_instance"].run_id, "error": error,
               "log_url": context["task_instance"].log_url},
    )
    try:
        audit.finish_run(run_id, "FAILED", error_message=error)
    except Exception:
        pass
    push_batch_metrics(
        dag_id, {"task_failed": 1}, labels={"dag_id": dag_id, "task_id": task_id}
    )


def on_sla_miss(*args, **kwargs) -> None:  # noqa: ANN002, ANN003
    log.error("VI PHAM SLA", extra={"event": "sla_miss", "args": str(args)[:500]})


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_exponential_backoff": True,
    "depends_on_past": False,
    "on_execute_callback": on_task_start,
    "on_success_callback": on_task_success,
    "on_failure_callback": on_task_failure,
}
