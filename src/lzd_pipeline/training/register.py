"""Dang ky booster uplift vao MLflow Model Registry.

Model duoc HUAN LUYEN o ngoai repo nay (notebook `lzd-uplifting-model`,
DRLearner + LightGBM). Cai vao day la mot **artifact da chot**, khong phai
mot lan train. Module nay dua artifact do vao registry de:

    - `inference-api` nap duoc qua `MlflowUpliftModel` thay vi doc file bundled
    - moi lan doi model deu co version, co run, co the roll back
    - metric cua ban goc di kem model chu khong nam roi trong mot file JSON

🚫 KHONG phai `train.py`. `train.py` la cho huan luyen TRONG repo va van con
   bon `TODO(model)` — hai viec khac nhau, khong duoc gop.

★ BOOSTER VA HOP DONG PHAI DI CUNG MOT RUN
--------------------------------------------------------------------------
`feature_contract.json` quyet dinh THU TU 76 cot dua vao model. Log rieng hai
thu ra hai cho la mo duong cho chung lech nhau, ma lech thu tu thi LightGBM
khong bao loi — no tra ve so sai. Nen ca hai nam trong cung `artifact_path`.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import configure_logging, get_logger
from lzd_pipeline.serving.feature_contract import load_contract

log = get_logger(__name__)

MODEL_NAME = os.environ.get("MODEL_NAME", "uplift_voucher")
MODEL_DIR = Path(__file__).resolve().parents[3] / "models" / "uplift_voucher"
BOOSTER = MODEL_DIR / "model_booster.txt"
CONTRACT = MODEL_DIR / "feature_contract.json"
METADATA = MODEL_DIR / "metadata.json"

#: Metric cua ban goc, do tren `rct_holdout`. Log kem de so sanh duoc khi
#: sau nay co model moi — khong phai de khoe.
_HOLDOUT_METRICS = ("qini", "auuc", "uplift_at_10", "uplift_at_20", "uplift_at_30")


def _verify_artifacts() -> tuple[Any, dict[str, Any]]:
    """Kiem TRUOC khi dang ky. Dang ky mot model sai con te hon khong dang ky."""
    for path in (BOOSTER, CONTRACT, METADATA):
        if not path.exists():
            raise FileNotFoundError(f"thieu artifact: {path}")

    contract = load_contract(CONTRACT)          # tu validate thu tu + so cot
    metadata = json.loads(METADATA.read_text(encoding="utf-8"))

    import lightgbm as lgb

    booster = lgb.Booster(model_str=BOOSTER.read_text(encoding="utf-8"))
    if booster.num_feature() != len(contract.order):
        raise ValueError(
            f"booster nhan {booster.num_feature()} cot, hop dong khai bao "
            f"{len(contract.order)} — 🚫 khong dang ky."
        )

    # 55 cot Redis phai khop scope reconstruction, neu khong feature store se
    # thieu dung cot ma model can.
    from lzd_pipeline.reconstruction.feature_set import load_feature_set

    scope = set(load_feature_set().columns)
    redis_cols = set(contract.redis_columns)
    if redis_cols != scope:
        raise ValueError(
            "55 cot Redis cua model khong khop selected feature set.\n"
            f"  model can, scope thieu : {sorted(redis_cols - scope)}\n"
            f"  scope co, model khong dung: {sorted(scope - redis_cols)}"
        )
    return contract, metadata


def register(
    *,
    stage_alias: str = os.environ.get("MODEL_STAGE", "Production"),
    run_name: str | None = None,
) -> dict[str, Any]:
    """Log booster + hop dong thanh mot MLflow run, roi dang ky model version."""
    import mlflow
    from mlflow.tracking import MlflowClient

    contract, metadata = _verify_artifacts()
    settings = get_settings()
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    mlflow.set_experiment(settings.mlflow_experiment)

    holdout = metadata.get("metric_rct_holdout", {}).get(metadata["ten_mo_hinh"], {})

    with mlflow.start_run(run_name=run_name or f"register-{contract.version}") as run:
        mlflow.log_params({
            "model_type": metadata["ten_mo_hinh"],
            "contract_version": contract.version,
            "n_model_features": len(contract.order),
            "n_redis_features": len(contract.redis_columns),
            "n_derived_features": len(contract.derived_columns),
            "n_default_features": len(contract.default_columns),
            "trained_at": metadata.get("ngay_huan_luyen"),
            "trained_rows": metadata.get("du_lieu_huan_luyen", {}).get("so_dong"),
            "source": "lzd-uplifting-model (notebook)",
            **{f"hp_{k}": v for k, v in metadata.get("sieu_tham_so", {}).items()},
        })
        mlflow.log_metrics({
            f"holdout_{k}": float(holdout[k]) for k in _HOLDOUT_METRICS if k in holdout
        })

        # ★ Booster + hop dong CUNG mot artifact_path — xem docstring module.
        mlflow.log_artifact(str(BOOSTER), artifact_path="model")
        mlflow.log_artifact(str(CONTRACT), artifact_path="model")
        mlflow.log_artifact(str(METADATA), artifact_path="model")
        # Danh sach 55 cot ma DE phai cap — feature store doc cai nay.
        mlflow.log_dict(
            {"redis_features": list(contract.redis_columns),
             "model_order": list(contract.order),
             "contract_version": contract.version},
            "model/redis_features.json",
        )

        model_uri = f"runs:/{run.info.run_id}/model"
        version = mlflow.register_model(model_uri=model_uri, name=MODEL_NAME)

        client = MlflowClient()
        client.set_registered_model_alias(MODEL_NAME, stage_alias, version.version)

        result = {
            "run_id": run.info.run_id,
            "model_name": MODEL_NAME,
            "model_version": version.version,
            "alias": stage_alias,
            "contract_version": contract.version,
            "model_uri": model_uri,
            "holdout_metrics": holdout,
        }
        log.info("dang ky model xong", extra={"event": "model_registered", **{
            k: v for k, v in result.items() if k != "holdout_metrics"}})
        return result


def main() -> None:
    configure_logging(service=os.environ.get("SERVICE_NAME", "model-register"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--alias", default=os.environ.get("MODEL_STAGE", "Production"))
    parser.add_argument("--run-name", default=None)
    parser.add_argument("--verify-only", action="store_true",
                        help="chi kiem artifact, khong cham MLflow")
    args = parser.parse_args()

    if args.verify_only:
        contract, metadata = _verify_artifacts()
        print(json.dumps({
            "contract_version": contract.version,
            "model_type": metadata["ten_mo_hinh"],
            "n_model_features": len(contract.order),
            "n_redis_features": len(contract.redis_columns),
            "n_derived_features": len(contract.derived_columns),
            "n_default_features": len(contract.default_columns),
            "verified": True,
        }, indent=2, ensure_ascii=False))
        return

    print(json.dumps(register(stage_alias=args.alias, run_name=args.run_name),
                     indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
