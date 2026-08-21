"""DAG 30 - TRAINING. Chay 03:00 thu Hai hang tuan.

    kiem du lieu -> train -> promote co dieu kien -> bao API nap lai

Moi buoc deu that, khong con cho gi nua:

    check_training_data   doc mart, tu choi neu thieu treated/control
    train                 `training/train.py` -> DRLearner, log MLflow,
                          dang ky version + `feature_contract.json`
    promote_model         `training/promote.py` -> chi doi alias Production
                          khi qua tu kiem VA hon `qini` cua ban dang chay
    notify_serving        POST /admin/reload-model

★ DAG NAY PAUSED KHI TAO RA (`is_paused_upon_creation=True`)
--------------------------------------------------------------------------
Lich tuan chi bat dau chay sau khi co nguoi bo pause — chu y la mot lan
train doc het training mart, khong phai viec nen tu khoi dong tren may cua
nguoi vua clone repo ve. Bat tu dong bang bien moi truong:

    AIRFLOW_UNPAUSE_DAGS=30_train_uplift_model      # trong .env

★ MODEL O DAY KHONG THAY THE MODEL NOTEBOOK MOT CACH TU DONG
--------------------------------------------------------------------------
Model baseline an 30F; model do DAG nay train an contract 83 cot hien tai — xem
`training/contract.py`). `promote_model` se KHONG doi alias khi ban dang
chay khong co metric `qini` de so, ma model dang ky tu notebook bang
`register.py` chinh la truong hop do. Muon chuyen han sang model train
trong repo thi phai quyet dinh bang tay, sau khi benchmark.
"""
from __future__ import annotations

import os
import pendulum
from airflow.decorators import dag, task
from airflow.models import Param

from lzd_utils.callbacks import DEFAULT_ARGS

DOC = __doc__


@dag(
    dag_id="30_train_uplift_model",
    description="Train uplift model hang tuan + promote co dieu kien vao MLflow Registry",
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
        """DRLearner + LightGBM, danh gia tren split='test'. Xem `train.py`."""
        from lzd_pipeline.training.train import run_training

        params = context["params"]
        return run_training(
            dt=context["ds"],
            sample_limit=int(params["sample_limit"]) or None,
            register=bool(params["register_model"]),
        )

    @task
    def promote_model(result: dict) -> dict:
        """Doi alias Production — chi khi model moi qua tu kiem VA hon ban cu.

        Tu choi promote KHONG phai loi: train ra model kem hon la chuyen binh
        thuong. Bao cao quyet dinh nam trong log va trong XCom tra ve.
        """
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.training.promote import promote_if_better

        log = get_logger(__name__)
        run_id = result.get("run_id")
        if not result.get("artifact_uri"):
            # Chay voi register_model=False (che do thu) -> khong co version
            # nao trong registry de gan alias.
            log.info("bo qua promote: run nay khong dang ky model",
                     extra={"event": "promote_skipped", "run_id": run_id})
            return {**result, "promotion": {"promoted": False,
                                            "reason": "register_model=False"}}

        return {**result, "promotion": promote_if_better(run_id)}

    @task
    def notify_serving(result: dict) -> dict:
        """Deploy verified alias artifact, then reload API; rollback alias on failure."""
        from pathlib import Path

        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.serving.deployment import deploy_production_alias

        log = get_logger(__name__)
        promotion = result.get("promotion") or {}
        if not promotion.get("promoted"):
            # Alias khong doi => nap lai chi lam API doc lai dung model no dang
            # giu. Bo qua de khong dung den serving ma chang duoc gi.
            log.info("bo qua reload: alias Production khong doi",
                     extra={"event": "model_reload_skipped",
                            "reason": promotion.get("reason", "")})
            return {"reloaded": False, "reason": promotion.get("reason", "")}

        try:
            deployed = deploy_production_alias(
                root=Path("/opt/serving-models"),
                model_name=promotion["model_name"],
                alias=promotion["alias"],
                reload_url="http://inference-api:8000/admin/reload-model",
                reload_token=os.environ.get("MODEL_RELOAD_TOKEN"),
            )
            log.info("model da deploy co kiem soat",
                     extra={"event": "model_deployed", **deployed})
            return deployed
        except Exception as exc:
            from mlflow.tracking import MlflowClient
            from lzd_pipeline.common.config import get_settings

            client = MlflowClient(tracking_uri=get_settings().mlflow_tracking_uri)
            previous = promotion.get("current_version")
            if previous:
                client.set_registered_model_alias(
                    promotion["model_name"], promotion["alias"], previous
                )
            else:
                client.delete_registered_model_alias(
                    promotion["model_name"], promotion["alias"]
                )
            log.exception("deployment that bai; da rollback registry alias",
                          extra={"event": "model_deploy_rollback", "error": str(exc)})
            raise

    stats = check_training_data()
    result = train(stats)
    notify_serving(promote_model(result))


train_uplift_model()
