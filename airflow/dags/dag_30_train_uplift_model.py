"""DAG 30 - TRAINING (KHUNG CHO TEAMMATE).

Duong ong da noi day du: doc training_dataset -> train -> log MLflow ->
dang ky model -> bao cho inference-api nap lai. Chi thieu phan model that
(xem `src/lzd_pipeline/training/train.py`, cac ham danh dau TODO(model)).

DAG dang o trang thai `is_paused_upon_creation=True`. Khi teammate dien xong
3 ham TODO, bo pause la chay duoc ngay.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models import Param

from lzd_utils.callbacks import DEFAULT_ARGS

DOC = __doc__


@dag(
    dag_id="30_train_uplift_model",
    description="[KHUNG] Train uplift model + dang ky vao MLflow Registry",
    schedule="0 3 * * 1",                     # 03:00 thu Hai hang tuan
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 1},
    tags=["train", "ml", "lzd"],
    doc_md=DOC,
    is_paused_upon_creation=True,
    params={
        "sample_limit": Param(0, type="integer",
                              description="Gioi han so dong (0 = dung het). Dat 100000 khi thu."),
        "register_model": Param(True, type="boolean"),
    },
)
def train_uplift_model():

    @task
    def check_training_data(**context) -> dict:
        """Truoc khi train: dataset co du dong, co ca treated lan control chua."""
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.common.clients import duckdb_conn
        from lzd_pipeline.features.spec import load_feature_spec

        log = get_logger(__name__)
        spec = load_feature_spec()
        table = spec.offline["training_table"]

        with duckdb_conn(read_only=True) as con:
            rows, treated, positive = con.execute(
                f"""
                SELECT COUNT(*),
                       SUM({spec.treatment_column}),
                       SUM({spec.label_column})
                FROM {table}
                """
            ).fetchone()

        stats = {
            "table": table,
            "rows": int(rows),
            "treated": int(treated or 0),
            "control": int(rows - (treated or 0)),
            "positive": int(positive or 0),
            "treatment_ratio": float((treated or 0) / rows) if rows else 0.0,
            "conversion_rate": float((positive or 0) / rows) if rows else 0.0,
        }
        log.info("kiem tra du lieu train", extra={"event": "training_data_check", **stats})

        if stats["rows"] < 1000:
            raise ValueError(f"Chi co {stats['rows']} dong - qua it de train")
        if stats["treated"] == 0 or stats["control"] == 0:
            raise ValueError("Thieu nhom treated hoac control - khong tinh duoc uplift")
        return stats

    @task(execution_timeout=pendulum.duration(hours=3))
    def train(stats: dict, **context) -> dict:
        """>>> TODO(model): ham nay se chay khi teammate dien xong train.py <<<"""
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.training.train import run_training

        log = get_logger(__name__)
        params = context["params"]
        try:
            return run_training(
                dt=context["ds"],
                sample_limit=int(params["sample_limit"]) or None,
                register=bool(params["register_model"]),
            )
        except NotImplementedError as exc:
            log.warning(
                "phan model chua duoc trien khai - bo qua buoc train",
                extra={"event": "train_not_implemented", "detail": str(exc)},
            )
            # Skip thay vi fail: DAG con lai van chay duoc de demo duong ong
            from airflow.exceptions import AirflowSkipException

            raise AirflowSkipException(
                "train.py chua co model (TODO(model)). Dien xong roi bo pause DAG nay."
            )

    @task
    def promote_model(result: dict) -> dict:
        """TODO(model): dat alias Production cho model version vua train.

        Goi y:
            client = MlflowClient()
            client.set_registered_model_alias(name, "Production", version)
        Nen co dieu kien: chi promote neu AUUC > model dang chay.
        """
        from lzd_pipeline.common.logging_setup import get_logger

        log = get_logger(__name__)
        log.info("cho teammate dien promote_model",
                 extra={"event": "promote_placeholder", "run_id": result.get("run_id")})
        return result

    @task
    def notify_serving(result: dict) -> dict:
        """Goi /admin/reload-model de API nap model moi ma khong can restart."""
        import json
        import urllib.request

        from lzd_pipeline.common.logging_setup import get_logger

        log = get_logger(__name__)
        url = "http://inference-api:8000/admin/reload-model"
        try:
            req = urllib.request.Request(url, method="POST", data=b"{}",
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = json.loads(resp.read())
            log.info("da bao inference-api nap lai model",
                     extra={"event": "model_reload_notified", **body})
            return body
        except Exception as exc:
            log.warning("khong goi duoc inference-api",
                        extra={"event": "model_reload_failed", "error": str(exc)})
            return {"error": str(exc)}

    stats = check_training_data()
    result = train(stats)
    notify_serving(promote_model(result))


train_uplift_model()
