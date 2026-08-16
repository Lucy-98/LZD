"""Hop dong dac trung cua model uplift — doc `models/uplift_voucher/feature_contract.json`.

★ MODULE NAY PHUC VU HAI DANG HOP DONG
--------------------------------------------------------------------------
    76 cot   model tu notebook (artifact bundled) — mo ta ngay duoi day
    62 cot   model do `train.py` huan luyen, sinh boi `training/contract.py`

Ca hai deu di qua `load_contract()` va `build_matrix()` — do la ly do khong
duoc hardcode con so 76 o bat ky dau trong duong suy luan.

Hop dong bundled: model nhan **76 cot**, xep theo `thu_tu_dua_vao_mo_hinh`.
Ba nguon:

    55  tu Redis           DE tinh va luu (dung bang scope fs_2026_08_v2)
     7  fe_* dan xuat      service TU TINH tu cot goc
    14  dien mac dinh      trung vi tren train+val
    --
    76

★ THU TU COT LA MOT PHAN CUA HOP DONG
--------------------------------------------------------------------------
Contract ghi nguyen van: *"Sai thu tu cot se cho du doan sai ma KHONG bao loi"*.
LightGBM nhan mang so, khong nhan ten — dua sai thu tu thi no van chay, van tra
ve so, chi la so sai. Do la ly do module nay dung thu tu tu ARTIFACT chu khong
chep tay, va `build_matrix()` la duong DUY NHAT de dung vector dau vao.

★ 14 COT MAC DINH — quyet dinh co y, khong phai thieu sot
--------------------------------------------------------------------------
9 trong 14 cot (`f48 f49 f50 f51 f55 f56 f60 f61 f63`) la muc one-hot ma
pipeline reconstruction CO tinh (`intermediate_only` cua fs_2026_08_v2).
Van dien mac dinh de du doan trung khit voi ban da benchmark.

`[MEASURED]` Cai gia phai tra: mot so user co CA GROUP one-hot bang 0 — trang
thai khong ton tai trong du lieu huan luyen.

    g2 (f43..f52)     13,508 / 926,669   1.46%
    g3 (f53..f62)    244,475 / 926,669  26.38%   <- dang ke
    g4 (f63..f65)     28,951 / 926,669   3.12%

🚫 Doi sang gia tri that se doi du doan => phai do lai Qini tren rct_holdout
   truoc khi tin. Xem `docs/UPLIFT_MODEL.md`.
"""
from __future__ import annotations

import functools
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

DEFAULT_PATH = (
    Path(__file__).resolve().parents[3] / "models" / "uplift_voucher" / "feature_contract.json"
)

#: `nguon_gia_tri` cua cot lay tu feature store.
SOURCE_REDIS = "redis"


class ContractError(ValueError):
    """Hop dong khong nhat quan. 🚫 KHONG fallback — du doan sai im lang con
    te hon khong du doan."""


@dataclass(frozen=True)
class FeatureSlot:
    """Mot cot dau vao cua model."""

    name: str
    index: int                    # thu_tu_dua_vao_mo_hinh
    from_redis: bool
    derived: bool                 # nguon != "goc"  =>  service tu tinh
    formula: str | None
    default: float


@dataclass(frozen=True)
class ModelFeatureContract:
    version: str
    model_name: str
    slots: tuple[FeatureSlot, ...]          # da sap theo index

    # -- nhom cot ----------------------------------------------------------
    @functools.cached_property
    def order(self) -> tuple[str, ...]:
        """76 ten cot, dung thu tu dua vao model."""
        return tuple(s.name for s in self.slots)

    @functools.cached_property
    def redis_columns(self) -> tuple[str, ...]:
        """55 cot DE phai luu len Redis — phai khop `fs_2026_08_v2`."""
        return tuple(s.name for s in self.slots if s.from_redis and not s.derived)

    @functools.cached_property
    def derived_columns(self) -> tuple[str, ...]:
        """7 cot fe_* service tu tinh."""
        return tuple(s.name for s in self.slots if s.derived)

    @functools.cached_property
    def default_columns(self) -> tuple[str, ...]:
        """14 cot dien gia tri mac dinh."""
        return tuple(s.name for s in self.slots if not s.from_redis)

    @functools.cached_property
    def defaults(self) -> Mapping[str, float]:
        return {s.name: s.default for s in self.slots}

    def slot_of(self, name: str) -> FeatureSlot:
        for s in self.slots:
            if s.name == name:
                return s
        raise ContractError(f"{name} khong co trong hop dong {self.version}")


# ===========================================================================
# 7 dac trung dan xuat — cai dat DUNG cong thuc trong contract
# ===========================================================================
#: Cot dem cua `fe_nonzero_top10`, doc tu contract luc kiem tra.
_NONZERO_TOP10 = ("f9", "f16", "f27", "f26", "f14", "f3", "f13", "f10", "f17", "f7")
#: Ba co nhi phan cua `fe_flag_sum`.
_FLAG_COLS = ("f66", "f68", "f75")


def _num(row: Mapping[str, Any], col: str, defaults: Mapping[str, float]) -> float:
    """Doc mot cot goc. Thieu / None / khong parse duoc => gia tri mac dinh.

    🚫 KHONG mac dinh ve 0.0: contract chot mac dinh la TRUNG VI tren train+val.
       Dung 0.0 cho `f36` (trung vi 0.965) se day mau ra ngoai mien huan luyen.
    """
    value = row.get(col)
    if value is None:
        return float(defaults.get(col, 0.0))
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(defaults.get(col, 0.0))
    return float(defaults.get(col, 0.0)) if math.isnan(out) else out


def compute_derived(
    row: Mapping[str, Any], defaults: Mapping[str, float]
) -> dict[str, float]:
    """7 dac trung fe_* — contract giao cho service tu tinh.

    ⚠️ Hai trong bay cot phu thuoc `f14`, ma `f14` nam trong 14 cot dien mac
    dinh (= 0.0). Nen `fe_ratio_f14_f16` va `fe_inter_f14_f27` LUON bang 0
    o production. Day la he qua cua hop dong, khong phai loi cai dat — ghi
    ra day de khong ai mat cong debug lai.
    """
    f = functools.partial(_num, row, defaults=defaults)
    return {
        "fe_ratio_f1_f2": f("f1") / (f("f2") + 1.0),
        "fe_ratio_f14_f16": f("f14") / (f("f16") + 1.0),
        "fe_ratio_f9_f27": f("f9") / (f("f27") + 1.0),
        "fe_inter_f9_f26": f("f9") * f("f26"),
        "fe_inter_f14_f27": f("f14") * f("f27"),
        "fe_nonzero_top10": float(sum(1 for c in _NONZERO_TOP10 if f(c) != 0.0)),
        "fe_flag_sum": float(sum(f(c) for c in _FLAG_COLS)),
    }


# ===========================================================================
# Dung ma tran dau vao
# ===========================================================================
def build_row(contract: ModelFeatureContract, row: Mapping[str, Any]) -> list[float]:
    """Mot ban ghi feature -> vector 76 chieu, DUNG thu tu cua model."""
    defaults = contract.defaults
    derived = compute_derived(row, defaults)

    out: list[float] = []
    for slot in contract.slots:
        if slot.derived:
            out.append(float(derived[slot.name]))
        elif slot.from_redis:
            out.append(_num(row, slot.name, defaults))
        else:
            out.append(float(slot.default))
    return out


def build_matrix(
    contract: ModelFeatureContract, rows: Sequence[Mapping[str, Any]]
) -> list[list[float]]:
    """Duong DUY NHAT de dung dau vao model. Xem canh bao thu tu o docstring."""
    return [build_row(contract, r) for r in rows]


# ===========================================================================
# Nap + kiem hop dong
# ===========================================================================
def _validate(c: ModelFeatureContract, raw: Mapping[str, Any]) -> None:
    indices = [s.index for s in c.slots]
    if indices != list(range(len(c.slots))):
        raise ContractError(
            f"`thu_tu_dua_vao_mo_hinh` phai la 0..{len(c.slots)-1} lien tuc, "
            f"dang thieu/trung: {sorted(set(range(len(c.slots))) - set(indices))}"
        )
    if len(c.slots) != int(raw["so_cot_mo_hinh_nhan_vao"]):
        raise ContractError(
            f"so cot lech: khai bao {raw['so_cot_mo_hinh_nhan_vao']}, dem duoc {len(c.slots)}"
        )
    if len(c.redis_columns) != int(raw["chia_viec"]["DE_tinh_va_luu_redis"]):
        raise ContractError(
            f"so cot redis lech: khai bao {raw['chia_viec']['DE_tinh_va_luu_redis']}, "
            f"dem duoc {len(c.redis_columns)}"
        )
    if len(c.default_columns) != int(raw["so_dac_trung_dien_mac_dinh"]):
        raise ContractError(
            f"so cot mac dinh lech: khai bao {raw['so_dac_trung_dien_mac_dinh']}, "
            f"dem duoc {len(c.default_columns)}"
        )
    declared = set(raw["chia_viec"]["AI_service_tu_tinh"])
    if set(c.derived_columns) != declared:
        raise ContractError(
            f"cot dan xuat lech: contract noi {sorted(declared)}, "
            f"doc duoc {sorted(c.derived_columns)}"
        )
    # Cai dat cua `compute_derived` phai PHU HET cot dan xuat da khai bao.
    #
    # Chi kiem chieu "thieu", khong kiem "thua": model do `train.py` huan
    # luyen an 62 cot doc thang tu store va KHONG dung cot fe_* nao, nen hop
    # dong cua no khai bao 0 cot dan xuat. Doi hoi bang nhau se lam moi hop
    # dong khong phai cua notebook deu bi tu choi. `build_row()` chi doc cot
    # co trong hop dong, nen cai dat tinh du ra la vo hai.
    computed = set(compute_derived({}, c.defaults))
    uncomputable = set(c.derived_columns) - computed
    if uncomputable:
        raise ContractError(
            f"hop dong khai bao cot dan xuat ma `compute_derived` khong tinh "
            f"duoc: {sorted(uncomputable)}"
        )


@functools.lru_cache(maxsize=4)
def load_contract(path: Path | str = DEFAULT_PATH) -> ModelFeatureContract:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    slots = tuple(sorted(
        (
            FeatureSlot(
                name=str(f["ten"]),
                index=int(f["thu_tu_dua_vao_mo_hinh"]),
                from_redis=f["nguon_gia_tri"] == SOURCE_REDIS,
                derived=f.get("nguon") != "goc",
                formula=f.get("cong_thuc"),
                default=float(f["gia_tri_mac_dinh"]),
            )
            for f in raw["dac_trung"]
        ),
        key=lambda s: s.index,
    ))
    contract = ModelFeatureContract(
        version=str(raw["phien_ban"]),
        model_name=str(raw["mo_hinh"]),
        slots=slots,
    )
    _validate(contract, raw)
    return contract
