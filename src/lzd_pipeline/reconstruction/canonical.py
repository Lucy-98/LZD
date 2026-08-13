"""Canonical form + hash — RECONSTRUCTION_SPEC.md §6.

Hai hash, LONG NHAU, dung chung MOT primitive:

    canonical_feature_payload   <- dinh nghia MOT lan
        |
        +--> feature_payload_hash = H(payload)
        +--> target_hash          = H(set_id, feature_version, reference_ts, payload)

🚫 CAM dung hash lam phep so feature equality (§6.2).
   Regime LN lech bit cuoi o ~7% dong DU reconstruction hoan toan dung
   => reconstructed_hash != target_hash la BINH THUONG, khong phai loi.
"""
from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from datetime import datetime

# Dau phan cach khong xuat hien trong du lieu (ASCII control chars)
_FIELD_SEP = "\x1f"   # giua cac thanh phan identity
_ITEM_SEP = "\x1e"    # giua cac cap col=value


def float_repr(x: float) -> str:
    """Bieu dien float64 round-trip duoc.

    `.17g` dam bao doc lai ra dung bit. NaN/Inf duoc chuan hoa tuong minh vi
    `format()` cho ra chuoi khac nhau tuy nen tang.
    """
    v = float(x)
    if math.isnan(v):
        return "nan"
    if math.isinf(v):
        return "inf" if v > 0 else "-inf"
    return format(v, ".17g")


def canonical_feature_payload(values: Mapping[str, float], columns: tuple[str, ...]) -> str:
    """Chuan hoa CHI phan payload.

    `columns` phai duoc sap theo ten (khong theo thu tu file) — day la thu
    quyet dinh hash on dinh giua cac lan chay.
    """
    missing = set(columns) - set(values)
    if missing:
        raise ValueError(f"thieu cot khi chuan hoa payload: {sorted(missing)}")
    return _ITEM_SEP.join(f"{c}={float_repr(values[c])}" for c in columns)


def canonical_target_record(
    *,
    selected_feature_set_id: str,
    feature_version: str,
    reference_ts: datetime,
    payload: str,
) -> str:
    """Identity BOC NGOAI payload (§6.1)."""
    return _FIELD_SEP.join(
        [selected_feature_set_id, feature_version, reference_ts.isoformat(), payload]
    )


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def feature_payload_hash(values: Mapping[str, float], columns: tuple[str, ...]) -> str:
    """Doi khi CHI payload doi. Dung de so hai target khac reference_ts."""
    return _sha256(canonical_feature_payload(values, columns))


def target_hash(
    *,
    values: Mapping[str, float],
    columns: tuple[str, ...],
    selected_feature_set_id: str,
    feature_version: str,
    reference_ts: datetime,
) -> str:
    """Tamper-evident hash cua CANONICAL TARGET RECORD (§6.1a).

    Doi khi payload HOAC identity doi. Dung de phat hien target bi sua.
    """
    return _sha256(
        canonical_target_record(
            selected_feature_set_id=selected_feature_set_id,
            feature_version=feature_version,
            reference_ts=reference_ts,
            payload=canonical_feature_payload(values, columns),
        )
    )


def reproducibility_fingerprint(
    *,
    target_hash_: str,
    selected_feature_set_id: str,
    feature_spec_version: str,
    constraint_model_version: str,
    encoding_version: str,
    objective_version: str,
    selection_policy_version: str,
    solver_version: str,
    semantic_branch: str | None,
    reference_ts: datetime,
    generation_seed: int,
) -> str:
    """Gate G — §11.

    `objective_version` va `selection_policy_version` BAT BUOC co mat:
    thieu chung thi doi objective / doi policy ma fingerprint khong doi
    => hai ket qua khac nhau mang cung fingerprint => Gate G mat hieu luc.

    `semantic_status` CO CHU Y khong nam trong fingerprint — no la metadata
    nhan thuc, khong duoc anh huong output (INVARIANT 2, TEST-08).
    """
    return _sha256(
        _FIELD_SEP.join(
            [
                target_hash_,
                selected_feature_set_id,
                feature_spec_version,
                constraint_model_version,
                encoding_version,
                objective_version,
                selection_policy_version,
                solver_version,
                semantic_branch or "",
                reference_ts.isoformat(),
                str(generation_seed),
            ]
        )
    )
