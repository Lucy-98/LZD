"""Huan luyen DR-Learner trong repo.

    python -m lzd_pipeline.training.train --dt 2026-08-05
    Airflow: DAG `30_train_uplift_model`

Duong ong (doc training data -> MLflow run -> log metric -> dang ky model) da
co san. Bon hook model gio noi thang vao `training/uplift.py` — ban port
nguyen van tu notebook `lzd-uplifting-model`.

★ QUAN HE VOI MODEL DANG PHUC VU
--------------------------------------------------------------------------
`models/uplift_voucher/model_booster.txt` sinh ra tu notebook, KHONG phai tu
file nay. Hai duong dung CUNG mot cai dat (`training/uplift.py`) nen ket qua
so sanh duoc, nhung KHONG the trung khit: notebook train tren 76 dac trung
(`train.parquet` + `val.parquet`, co 14 cot ma feature store khong cap), con
file nay train tren dung tap cot ma `feature_spec.yml` khai bao.

    huan luyen o day    => model MOI, phai benchmark lai truoc khi thay
    nap artifact        => model DANG PHUC VU, xem `training/register.py`

🚫 Dung `register.py` de dua model da chot vao registry. Dung `train.py` de
   huan luyen ban moi. Gop hai viec lai se lam khong ai biet model dang chay
   den tu dau.

★ RUN NAY PHUC VU DUOC — NHUNG KHONG TU DONG THAY MODEL CU
--------------------------------------------------------------------------
Moi run dang ky deu kem `feature_contract.json` mo ta dung 83 cot hien tai
(`training/contract.py`), nen `MlflowUpliftModel` nap duoc thang tu registry.
Con viec no CO duoc phuc vu hay khong la quyet dinh rieng cua
`training/promote.py`: alias `Production` chi doi khi qua tu kiem va hon
`qini` cua ban dang chay.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import configure_logging, get_logger
from lzd_pipeline.features.spec import load_feature_spec
from lzd_pipeline.training import contract, dataset as ds

log = get_logger(__name__)

MODEL_NAME = "uplift_voucher"


# ===========================================================================
# 1) Khoi tao model
# ===========================================================================
def build_model(params: dict[str, Any]):
    """DR-Learner (Kennedy 2020) voi base learner LightGBM.

    `params` mac dinh la bo Optuna da tim (`uplift.BEST_PARAMS`). Truyen bo
    khac thi phai bench lai — sieu tham so duoc tune theo muc tieu `dr_qini`
    tren chinh phan phoi nay.
    """
    from lzd_pipeline.training.uplift import BEST_PARAMS, DRLearner

    return DRLearner(n_folds=5, params={**BEST_PARAMS, **(params or {})})


# ===========================================================================
# 2) Huan luyen
# ===========================================================================
def fit_model(model, x, y, treatment) -> Any:
    """`DRLearner.fit(X, W, Y)` — chu y THU TU: treatment truoc, outcome sau.

    Notebook dat chu ky la `fit(X, W, Y)`. Doi cho hai doi so nay se cho ra
    mot model van chay, van co metric, chi la hoc nham bien — nen giu nguyen
    thu tu va goi bang ten.
    """
    import numpy as np

    return model.fit(
        np.asarray(x, dtype=float),
        np.asarray(treatment).astype(int),
        np.asarray(y).astype(int),
    )


# ===========================================================================
# 3) Danh gia
# ===========================================================================
def evaluate(model, x, y, treatment) -> dict[str, float]:
    """Qini / AUUC / uplift@k — cai dat port tu notebook (`uplift.evaluate_all`).

    Kem bon phep tu kiem cua notebook: metric uplift rat de cai dat sai ma van
    cho ra so dep, nen no phai chung minh la no dung truoc khi ta tin con so.
    """
    from lzd_pipeline.training.uplift import evaluate_all, sanity_checks

    score = predict_uplift(model, x)
    metrics = evaluate_all(y, treatment, score)

    checks = sanity_checks(y, treatment, score)
    metrics.update({f"sanity_{k}": float(v) for k, v in checks.items()})
    failed = [k for k, v in checks.items() if not v]
    if failed:
        log.warning("thuoc do khong qua tu kiem",
                    extra={"event": "metric_sanity_failed", "failed": failed})
    return metrics


def predict_uplift(model, x):
    """CATE score. inference-api KHONG goi ham nay — no nap booster qua
    `serving/model_loader.py`. Day chi dung trong huan luyen/danh gia."""
    import numpy as np

    return model.predict_cate(np.asarray(x, dtype=float))


# ===========================================================================
# Duong ong huan luyen - DA HOAN CHINH, khong can sua
# ===========================================================================
#: Dai cua so huan luyen, PHAI khop `training_window_weeks` trong
#: `dbt_project.yml` — dbt chi giu bay nhieu tuan trong bang, doc rong hon
#: cung khong co them dong nao.
TRAIN_WINDOW_WEEKS = 12


def run_training(
    dt: str,
    params: dict[str, Any] | None = None,
    sample_limit: int | None = None,
    register: bool = True,
    window_weeks: int = TRAIN_WINDOW_WEEKS,
) -> dict[str, Any]:
    import mlflow

    settings = get_settings()
    spec = load_feature_spec()
    # Mac dinh la bo Optuna da tim (12 trial, muc tieu `dr_qini`) — KHONG phai
    # so tuy chon. Doi thi phai bench lai.
    from lzd_pipeline.training.uplift import BEST_PARAMS, E_ALPHA

    params = {**BEST_PARAMS, **(params or {})}

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    mlflow.set_experiment(settings.mlflow_experiment)

    started = time.time()
    with mlflow.start_run(run_name=f"uplift-{dt}") as run:
        # -------- 1. Du lieu -------------------------------------------
        # Cua so TUONG MINH. Doc ca bang thi luong du lieu phu thuoc am tham
        # vao viec dbt duoc goi the nao lan cuoi — hai run co the thay hai
        # luong khac han ma params nhin y het.
        dt_from, dt_to = ds.training_window(dt, window_weeks)
        train_df = ds.load_training_frame(
            split="train", dt_from=dt_from, dt_to=dt_to,
            limit=sample_limit, spec=spec,
        )
        # Danh gia tren tap DONG BANG, khong phai `split='test'` cua bang
        # training. Xem `dbt/models/marts/eval_holdout.sql`.
        test_df, holdout_version = ds.load_eval_holdout(limit=sample_limit, spec=spec)
        stats = ds.dataset_stats(train_df, spec)

        mlflow.log_params({
            **params,
            "dt": dt,
            "model_type": "DRLearner",
            "propensity_alpha": E_ALPHA,
            "feature_spec_version": spec.version,
            "n_features": len(ds.feature_columns(spec)),
            # ★ Bon truong duoi day tra loi cau "run nay da thay du lieu gi".
            # Thieu chung thi hai run khac nhau ve du lieu van nhin giong het
            # nhau trong MLflow.
            "train_window_weeks": window_weeks,
            "dt_from": dt_from,
            "dt_to": dt_to,
            "n_train": len(train_df),
            # `promote.py` TU CHOI so hai run khac holdout_version.
            "holdout_version": holdout_version,
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

        # -------- 2. Model ----------------------------------------------
        x_tr, y_tr, t_tr = ds.split_xyt(train_df, spec)
        x_te, y_te, t_te = ds.split_xyt(test_df, spec)

        model = build_model(params)
        model = fit_model(model, x_tr, y_tr, t_tr)
        # Danh gia tren tap RCT DONG BANG. 🚫 Khong duoc dung no de chon sieu
        # tham so, va khong duoc de no lon len theo tung tuan — `promote.py`
        # so qini giua cac run, phep so do chi co nghia tren cung mot tap.
        metrics = evaluate(model, x_te, y_te, t_te)

        # -------- 3. Log & dang ky -------------------------------------
        mlflow.log_metrics(metrics)
        mlflow.log_metric("train_duration_sec", time.time() - started)

        artifact_uri = None
        if register:
            # ★ Log BOOSTER TEXT, khong phai pickle.
            #
            # `serving/model_loader.py` nap booster text. Log pickle o day se
            # tao hai dinh dang cho cung mot vai tro, va ban pickle lai khoa
            # cung phien ban Python (da do: nap ban 3.10 bang 3.14 thi do).
            # Cung dinh dang => model tu `train.py` va model tu `register.py`
            # thay the duoc cho nhau.
            #
            # ★ BOOSTER + HOP DONG PHAI CUNG MOT `artifact_path`.
            # `MlflowUpliftModel.load()` tai ca hai tu cung run. Thieu hop dong
            # thi no nem loi va API tut ve booster bundled — DAG van xanh, model
            # moi khong bao gio duoc phuc vu.
            import tempfile

            with tempfile.TemporaryDirectory() as tmp:
                artifact_dir = Path(tmp)
                (artifact_dir / "model_booster.txt").write_text(
                    model.booster_text(), encoding="utf-8")
                model_contract = contract.build_contract(
                        train_df, spec,
                        version=f"{dt}-{run.info.run_id[:8]}",
                        run_id=run.info.run_id,
                )
                contract.write_contract(model_contract, tmp)
                model_order = [item["ten"] for item in model_contract["dac_trung"]]
                default_row = {
                    item["ten"]: item["gia_tri_mac_dinh"]
                    for item in model_contract["dac_trung"]
                }
                sentinel_score = float(
                    predict_uplift(model, [[default_row[n] for n in model_order]])[0]
                )
                hashes = {}
                for name in ("model_booster.txt", "feature_contract.json"):
                    hashes[name] = hashlib.sha256((artifact_dir / name).read_bytes()).hexdigest()
                (artifact_dir / "import_manifest.json").write_text(
                    json.dumps({
                        "source_repository": "lzd_pipeline.training.train",
                        "source_commit": run.info.run_id,
                        "model_name": MODEL_NAME,
                        "model_type": "DRLearner-LightGBM",
                        "feature_count": len(model_order),
                        "decision_threshold": settings.uplift_threshold,
                        "sentinel": {
                            "input": "feature_contract_defaults",
                            "expected_uplift": sentinel_score,
                            "absolute_tolerance": 1e-12,
                        },
                        "compatibility": {
                            "feature_spec_version": (
                                spec.offline.get("selected_feature_set_id")
                                or f"feature_spec_v{spec.version}"
                            ),
                            "requires_realtime": any(
                                name in spec.realtime_names for name in model_order
                            ),
                            "realtime_semantics_version": "event_time_5m_1h_dedup_v1",
                        },
                        "sha256": hashes,
                    }, indent=2),
                    encoding="utf-8",
                )
                mlflow.log_artifacts(tmp, artifact_path="model")

            mlflow.register_model(
                model_uri=f"runs:/{run.info.run_id}/model", name=MODEL_NAME)
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
            "train_window": {"dt_from": dt_from, "dt_to": dt_to,
                             "weeks": window_weeks, "n_train": len(train_df)},
            "holdout_version": holdout_version,
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
