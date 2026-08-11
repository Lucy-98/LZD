"""=========================== KHUNG CHO TEAMMATE ===========================

PHAN NAY CHUA LAM MODEL - chi dung khung + duong ong.

Da lam san (khong can dong):
  - doc training data tu DuckDB dung dung feature theo feature_spec.yml
  - tao MLflow run, log param/metric/dataset stats
  - luu artifact len MinIO qua MLflow
  - dang ky model vao MLflow Model Registry (de inference-api load duoc)

Teammate can dien vao 3 cho danh dau `TODO(model)`:
  1. build_model()  - khoi tao estimator (S-learner / T-learner / X-learner /
                      DESCN theo paper trong docs/2207.09920v3.pdf)
  2. fit_model()    - huan luyen
  3. evaluate()     - tinh AUUC / Qini / uplift@k

Chay:
    python -m lzd_pipeline.training.train --dt 2026-08-05
Hoac qua Airflow: DAG `30_train_uplift_model`.
=========================================================================="""
from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import configure_logging, get_logger
from lzd_pipeline.features.spec import load_feature_spec
from lzd_pipeline.training import dataset as ds

log = get_logger(__name__)

MODEL_NAME = "uplift_voucher"


# ===========================================================================
# 1) TODO(model): khoi tao model
# ===========================================================================
def build_model(params: dict[str, Any]):
    """Tra ve doi tuong model chua train.

    Goi y trien khai (chon 1):

      a) T-learner voi LightGBM (don gian, chay duoc ngay):
             from lightgbm import LGBMClassifier
             return {"treated": LGBMClassifier(**params),
                     "control": LGBMClassifier(**params)}

      b) causalml:
             from causalml.inference.meta import BaseXClassifier

      c) DESCN (dung paper Lazada trong docs/2207.09920v3.pdf) - PyTorch,
         multi-task: propensity + ESTR + ESCR + pseudo treatment effect.

    Nho: them thu vien tuong ung vao docker/airflow/requirements.txt.
    """
    raise NotImplementedError(
        "TODO(model): chon va khoi tao uplift model o day. "
        "Xem goi y trong docstring cua build_model()."
    )


# ===========================================================================
# 2) TODO(model): huan luyen
# ===========================================================================
def fit_model(model, x, y, treatment) -> Any:
    """Huan luyen model. Tra ve model da fit."""
    raise NotImplementedError("TODO(model): huan luyen model o day.")


# ===========================================================================
# 3) TODO(model): danh gia
# ===========================================================================
def evaluate(model, x, y, treatment) -> dict[str, float]:
    """Tra ve dict metric. Toi thieu nen co:

        auuc              - Area Under Uplift Curve
        qini              - Qini coefficient
        uplift_at_10pct   - uplift trung binh o top 10% score cao nhat
        auc_response      - AUC cua nhanh du doan conversion

    Cac metric nay se tu dong duoc log len MLflow va hien tren dashboard.
    """
    raise NotImplementedError("TODO(model): tinh AUUC/Qini o day.")


def predict_uplift(model, x):
    """Suy luan uplift score. inference-api se goi ham nay qua model_loader.

    Voi T-learner:  p_treated(x) - p_control(x)
    """
    raise NotImplementedError("TODO(model): tra ve uplift score.")


# ===========================================================================
# Duong ong huan luyen - DA HOAN CHINH, khong can sua
# ===========================================================================
def run_training(
    dt: str,
    params: dict[str, Any] | None = None,
    sample_limit: int | None = None,
    register: bool = True,
) -> dict[str, Any]:
    import mlflow

    settings = get_settings()
    spec = load_feature_spec()
    params = params or {"n_estimators": 300, "learning_rate": 0.05, "num_leaves": 63}

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    mlflow.set_experiment(settings.mlflow_experiment)

    started = time.time()
    with mlflow.start_run(run_name=f"uplift-{dt}") as run:
        # -------- 1. Du lieu -------------------------------------------
        train_df = ds.load_training_frame(split="train", limit=sample_limit, spec=spec)
        test_df = ds.load_training_frame(split="test", limit=sample_limit, spec=spec)
        stats = ds.dataset_stats(train_df, spec)

        mlflow.log_params({
            **params,
            "dt": dt,
            "feature_spec_version": spec.version,
            "n_features": len(ds.feature_columns(spec)),
            "n_train": len(train_df),
            "n_test": len(test_df),
        })
        mlflow.log_metrics({f"data_{k}": v for k, v in stats.items()})
        # Luu dung danh sach + thu tu feature: inference PHAI dung y het
        mlflow.log_dict(
            {"features": ds.feature_columns(spec), "spec_version": spec.version},
            "feature_list.json",
        )

        log.info("bat dau train",
                 extra={"event": "train_start", "dt": dt, "mlflow_run_id": run.info.run_id,
                        **stats})

        # -------- 2. Model (phan cua teammate) --------------------------
        x_tr, y_tr, t_tr = ds.split_xyt(train_df, spec)
        x_te, y_te, t_te = ds.split_xyt(test_df, spec)

        model = build_model(params)          # TODO(model)
        model = fit_model(model, x_tr, y_tr, t_tr)   # TODO(model)
        metrics = evaluate(model, x_te, y_te, t_te)  # TODO(model)

        # -------- 3. Log & dang ky -------------------------------------
        mlflow.log_metrics(metrics)
        mlflow.log_metric("train_duration_sec", time.time() - started)

        artifact_uri = None
        if register:
            import mlflow.sklearn  # doi sang flavor tuong ung (pytorch/lightgbm...)

            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
                registered_model_name=MODEL_NAME,
            )
            artifact_uri = f"runs:/{run.info.run_id}/model"

        log.info("train xong",
                 extra={"event": "train_done", "mlflow_run_id": run.info.run_id,
                        "metrics": metrics, "artifact_uri": artifact_uri})

        return {
            "run_id": run.info.run_id,
            "model_name": MODEL_NAME,
            "artifact_uri": artifact_uri,
            "metrics": metrics,
            "data_stats": stats,
        }


def main() -> None:
    configure_logging(service=os.environ.get("SERVICE_NAME", "trainer"))
    parser = argparse.ArgumentParser(description="Train uplift model")
    parser.add_argument("--dt", required=True, help="Ngay logic, vd 2026-08-05")
    parser.add_argument("--limit", type=int, default=None, help="Gioi han so dong (dev)")
    parser.add_argument("--no-register", action="store_true")
    args = parser.parse_args()

    result = run_training(args.dt, sample_limit=args.limit, register=not args.no_register)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
