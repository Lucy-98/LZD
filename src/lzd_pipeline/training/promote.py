"""Dat alias `Production` cho model version — CO DIEU KIEN.

    python -m lzd_pipeline.training.promote --run-id <mlflow_run_id>
    Airflow: task `promote_model` cua DAG `30_train_uplift_model`

★ VI SAO PHAI CO CONG, KHONG PHAI CU TRAIN XONG LA DAY LEN
--------------------------------------------------------------------------
DAG train chay 03:00 thu Hai hang tuan va khong ai ngoi xem. Neu tuan nay
du lieu bi lech — dbt loi, treated/control mat can bang, feature store thieu
cot — thi model moi van train xong, van co metric, va neu tu dong promote thi
sang thu Hai production dang phuc vu mot model te hon ma khong ai biet.

Nen o day co ba cong, phai qua CA BA:

    1. tu kiem   moi `sanity_*` cua notebook phai dung. Metric uplift rat de
                 cai dat sai ma van cho so dep; day la cach chinh no noi ra.
    2. cung thuoc hai run phai co cung `holdout_version`. Doi tap danh gia ma
                 van so `qini` la do bang hai cai thuoc khac nhau.
    3. so sanh   `qini` phai hon ban dang giu alias Production.

Cong 2 dung truoc cong 3 co y: khong so duoc thi khong duoc so, chu khong
phai so roi moi hoi la co hop le khong.

Khong qua cong => giu nguyen alias cu, GHI RO ly do, va KHONG bao loi. Train
that bai la su co; train ra model kem hon la chuyen binh thuong, khong duoc
lam do ca DAG.

★ LAN DAU CHAY THI KHONG CO GI DE SO
--------------------------------------------------------------------------
Chua co version nao mang alias => promote thang. Do la cach duy nhat de
`Production` co ban dau; kiem tra chat luong tuyet doi la viec cua benchmark
tay, khong phai cua DAG hang tuan.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from lzd_pipeline.common.config import get_settings
from lzd_pipeline.common.logging_setup import configure_logging, get_logger

log = get_logger(__name__)

MODEL_NAME = os.environ.get("MODEL_NAME", "uplift_voucher")
ALIAS = os.environ.get("MODEL_STAGE", "Production")

#: Metric quyet dinh. `qini` chu khong phai `auuc`: Optuna cung tune theo
#: ho metric nay (`dr_qini`), nen promote theo no thi khong mau thuan voi
#: cai da dung de chon sieu tham so.
PROMOTION_METRIC = "qini"

#: Bien do toi thieu de coi la "hon". Bang 0 nghia la hon mot chut cung doi;
#: dat > 0 neu thay alias nhay qua thuong xuyen vi nhieu ngau nhien.
MIN_GAIN = float(os.environ.get("PROMOTION_MIN_GAIN", "0.0"))


def _client():
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(get_settings().mlflow_tracking_uri)
    return MlflowClient()


def _version_of_run(client, model_name: str, run_id: str) -> Any:
    """Version vua duoc `mlflow.register_model()` tao ra tu run nay."""
    versions = [
        v for v in client.search_model_versions(f"name='{model_name}'")
        if v.run_id == run_id
    ]
    if not versions:
        raise LookupError(
            f"khong tim thay version nao cua {model_name!r} sinh tu run {run_id!r} "
            "— run_training() co chay voi register=True khong?"
        )
    return max(versions, key=lambda v: int(v.version))


def _metric(client, run_id: str, name: str) -> float | None:
    try:
        return float(client.get_run(run_id).data.metrics[name])
    except (KeyError, TypeError, ValueError):
        return None


def _param(client, run_id: str, name: str) -> str | None:
    """Param cua run. `None` khi run cu khong log truong nay.

    Ban dang ky bang `register.py` (model tu notebook) khong co
    `holdout_version` — no do tren `rct_holdout` cua notebook, mot tap khac
    han. `None != 'h_2026...'` nen cong so sanh se tu choi, dung nhu mong
    muon: khong tu dong hat model da benchmark bang mot ban chua benchmark.
    """
    try:
        return client.get_run(run_id).data.params.get(name)
    except Exception:
        return None


def _failed_sanity(client, run_id: str) -> list[str]:
    """`train.evaluate()` log moi tu kiem duoi dang `sanity_<ten>` = 0.0/1.0."""
    metrics = client.get_run(run_id).data.metrics
    return sorted(
        k.removeprefix("sanity_")
        for k, v in metrics.items()
        if k.startswith("sanity_") and float(v) != 1.0
    )


def _deployment_package_gate(client, run_id: str) -> tuple[bool, str]:
    """Verify deploy metadata and active feature compatibility before alias swap."""
    try:
        manifest_path = Path(client.download_artifacts(run_id, "model/import_manifest.json"))
        contract_path = Path(client.download_artifacts(run_id, "model/feature_contract.json"))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        expected_contract_hash = (manifest.get("sha256") or {}).get("feature_contract.json")
        actual_contract_hash = hashlib.sha256(contract_path.read_bytes()).hexdigest()
        if not expected_contract_hash or expected_contract_hash != actual_contract_hash:
            return False, "feature contract checksum missing or invalid"

        from lzd_pipeline.features.compatibility import check_model_feature_compatibility
        from lzd_pipeline.features.online_store import OnlineFeatureStore
        from lzd_pipeline.features.spec import load_feature_spec
        from lzd_pipeline.serving.feature_contract import load_contract

        contract = load_contract(contract_path)
        compatibility = manifest.get("compatibility") or {}
        store = OnlineFeatureStore()
        version = store.get_active_version()
        if not version:
            return False, "Redis has no active feature version"
        report = check_model_feature_compatibility(
            model_features=contract.order,
            model_feature_spec_version=str(compatibility.get("feature_spec_version", "")),
            model_realtime_semantics_version=str(
                compatibility.get("realtime_semantics_version", "")
            ),
            requires_realtime=bool(compatibility.get("requires_realtime", False)),
            spec=load_feature_spec(),
            active_status=store.get_version_status(version),
        )
        return report.compatible, "; ".join(report.reasons)
    except Exception as exc:
        return False, f"deployment package gate failed: {exc}"


def promote_if_better(
    run_id: str,
    *,
    model_name: str = MODEL_NAME,
    alias: str = ALIAS,
    metric: str = PROMOTION_METRIC,
    min_gain: float = MIN_GAIN,
) -> dict[str, Any]:
    """Tra ve bao cao quyet dinh. KHONG nem loi khi tu choi promote."""
    client = _client()
    candidate = _version_of_run(client, model_name, run_id)
    new_score = _metric(client, run_id, metric)

    report: dict[str, Any] = {
        "model_name": model_name,
        "alias": alias,
        "metric": metric,
        "candidate_version": candidate.version,
        "candidate_metric": new_score,
        "current_version": None,
        "current_metric": None,
        "promoted": False,
        "reason": "",
    }

    failed = _failed_sanity(client, run_id)
    if failed:
        report["reason"] = f"tu kiem that bai: {failed}"
        log.warning("khong promote - metric khong qua tu kiem",
                    extra={"event": "promote_rejected", **report})
        return report

    if new_score is None:
        report["reason"] = f"run khong co metric {metric!r}"
        log.warning("khong promote - thieu metric",
                    extra={"event": "promote_rejected", **report})
        return report

    package_ok, package_reason = _deployment_package_gate(client, run_id)
    if not package_ok:
        report["reason"] = package_reason
        log.warning("khong promote - deployment package khong tuong thich",
                    extra={"event": "promote_rejected", **report})
        return report

    try:
        current = client.get_model_version_by_alias(model_name, alias)
    except Exception:
        current = None

    if current is None:
        client.set_registered_model_alias(model_name, alias, candidate.version)
        report.update(promoted=True, reason=f"chua co ban nao mang alias {alias!r}")
        log.info("promote lan dau", extra={"event": "promote_done", **report})
        return report

    old_score = _metric(client, current.run_id, metric)
    report["current_version"] = current.version
    report["current_metric"] = old_score

    # ★ CONG THU BA: hai run phai do tren CUNG mot tap danh gia.
    #
    # Neu tap danh gia doi (bump `holdout_version` trong `dbt_project.yml`,
    # hoac tap test lon len theo tung tuan) thi `qini` cua hai run la hai so
    # do bang hai cai thuoc khac nhau. So chung lai van ra mot ket qua, va cai
    # ket qua do van doi alias — do la kieu hong khong co trieu chung.
    #
    # Tu choi so con hon so sai. Muon so lai thi phai train lai ban dang chay
    # tren holdout moi.
    new_holdout = _param(client, run_id, "holdout_version")
    old_holdout = _param(client, current.run_id, "holdout_version")
    report["candidate_holdout"] = new_holdout
    report["current_holdout"] = old_holdout
    if new_holdout != old_holdout:
        report["reason"] = (
            f"hai run do tren hai tap danh gia khac nhau "
            f"({new_holdout!r} vs {old_holdout!r}) — khong so duoc"
        )
        log.warning("khong promote - khac tap danh gia",
                    extra={"event": "promote_rejected", **report})
        return report

    # Ban dang chay khong do duoc metric (vd model dang ky bang register.py tu
    # notebook, log ten metric khac) => khong co co so so sanh, giu nguyen.
    # Doi alias mu quang nguy hiem hon la khong doi.
    if old_score is None:
        report["reason"] = (
            f"ban dang chay (v{current.version}) khong co metric {metric!r} de so"
        )
        log.warning("khong promote - khong so sanh duoc",
                    extra={"event": "promote_rejected", **report})
        return report

    if new_score > old_score + min_gain:
        client.set_registered_model_alias(model_name, alias, candidate.version)
        report.update(
            promoted=True,
            reason=f"{metric} {new_score:.6f} > {old_score:.6f} (+{min_gain})",
        )
        log.info("promote model moi", extra={"event": "promote_done", **report})
    else:
        report["reason"] = (
            f"{metric} {new_score:.6f} khong hon ban v{current.version} "
            f"({old_score:.6f}, can +{min_gain})"
        )
        log.info("giu nguyen model dang chay",
                 extra={"event": "promote_skipped", **report})
    return report


def main() -> None:
    configure_logging(service=os.environ.get("SERVICE_NAME", "trainer"))
    parser = argparse.ArgumentParser(description="Promote model version len alias")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--model-name", default=MODEL_NAME)
    parser.add_argument("--alias", default=ALIAS)
    parser.add_argument("--metric", default=PROMOTION_METRIC)
    parser.add_argument("--min-gain", type=float, default=MIN_GAIN)
    args = parser.parse_args()

    print(json.dumps(
        promote_if_better(
            args.run_id,
            model_name=args.model_name,
            alias=args.alias,
            metric=args.metric,
            min_gain=args.min_gain,
        ),
        indent=2, ensure_ascii=False,
    ))


if __name__ == "__main__":
    main()
