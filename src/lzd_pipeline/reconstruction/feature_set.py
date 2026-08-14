"""Doc `fs_2026_08_v*.yaml` — SELECTED FEATURE SET contract.

RECONSTRUCTION_SPEC.md §13.3.

Day la nguon DUY NHAT cua reconstruction scope. Truoc day danh sach nay nam
duoi dang van xuoi o 3 tai lieu khac nhau -> khong co gi ngan chung drift (B5).

★ INVARIANT 1 van la hard invariant, chi khac cho DAT con so:
      truoc:  `if len(fs.columns) != 36`  — hard-code trong code
      nay:    `expected_column_count` trong CHINH artifact yaml

  Ly do doi: con so 36 nam trong code lam artifact khong the tu mo ta scope
  cua no. Khi v2 nang len 55, mot hang so trong code se bien INVARIANT 1
  thanh "cai chan duong" thay vi "cai bao ve" — va cach de nhat de vuot no
  la xoa han assert. Dat trong yaml thi doi scope BAT BUOC phai sua artifact
  va bump `id`, tuc la doi hop dong mot cach hien thi duoc trong git diff.

🚫 Module nay KHONG doc `feature_semantics_2026_08.yaml`. `semantic_signal_count`
   va cau hoi ngo f9/f16 KHONG duoc phep chan solver (§13.2).
"""
from __future__ import annotations

import functools
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml

Tier = Literal["T1", "T2", "T3"]
Regime = Literal["LOG10", "LN", "REC", "CAT", "PASS"]

DEFAULT_PATH = Path(__file__).resolve().parents[3] / "config" / "features" / "fs_2026_08_v2.yaml"


@dataclass(frozen=True)
class SourceAttribute:
    """Mot thuoc tinh nghiep vu synthetic sinh ra >=1 cot T2.

    `outputs` la TOAN BO cot ma attribute sinh ra, ke ca cot khong duoc chon.
    Group-level reconstruction (§5, G-1): phai dung DU muc, khong phai chi muc
    cua cot da chon -- neu khong, one-hot invariant cua group vo.
    """

    name: str
    levels: int
    encoding: str
    outputs: tuple[str, ...]
    selected: tuple[str, ...]

    def __post_init__(self) -> None:
        missing = set(self.selected) - set(self.outputs)
        if missing:
            raise ValueError(f"{self.name}: selected khong nam trong outputs: {sorted(missing)}")
        if self.levels < len(self.outputs) and self.encoding == "one_hot":
            raise ValueError(
                f"{self.name}: one_hot can levels >= so cot output "
                f"({self.levels} < {len(self.outputs)})"
            )


@dataclass(frozen=True)
class SelectedFeatureSet:
    id: str
    split_allowed: str
    expected_column_count: int
    tiers: dict[Tier, tuple[str, ...]]
    regimes: dict[Regime, tuple[str, ...]]
    tolerances: dict[Regime, float]
    source_attributes: dict[str, SourceAttribute]
    intermediate_only: frozenset[str]

    # -- scope --------------------------------------------------------------
    @functools.cached_property
    def columns(self) -> tuple[str, ...]:
        """Cot muc tieu, sap theo ten (thu tu canonical cho hashing §6.1)."""
        return tuple(sorted(c for cols in self.tiers.values() for c in cols))

    @functools.cached_property
    def column_set(self) -> frozenset[str]:
        return frozenset(self.columns)

    @functools.cached_property
    def gate_a_columns(self) -> tuple[str, ...]:
        """Cot T1+T2 — pham vi Gate A (§12)."""
        return tuple(sorted(self.tiers["T1"] + self.tiers["T2"]))

    @functools.cached_property
    def gate_a_t3_columns(self) -> tuple[str, ...]:
        """Cot T3 — pham vi Gate A-T3, bao cao ti le RIENG (§12)."""
        return tuple(sorted(self.tiers["T3"]))

    def tier_of(self, column: str) -> Tier:
        for tier, cols in self.tiers.items():
            if column in cols:
                return tier
        raise KeyError(f"{column} khong thuoc selected feature set {self.id}")

    def regime_of(self, column: str) -> Regime:
        for regime, cols in self.regimes.items():
            if column in cols:
                return regime
        raise KeyError(f"{column} khong co regime trong {self.id}")

    def tolerance_of(self, column: str) -> float | None:
        """Dung sai TUONG DOI. None = so sanh chinh xac.

        🚫 CAM noi dung sai cua regime LN (§7). Fail o 1e-15 => solver sai.
        """
        return self.tolerances.get(self.regime_of(column))


def _load_raw(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@functools.lru_cache(maxsize=4)
def load_feature_set(path: Path | str = DEFAULT_PATH) -> SelectedFeatureSet:
    raw = _load_raw(Path(path))

    tiers: dict[Tier, tuple[str, ...]] = {
        t: tuple(raw["columns"][t]) for t in ("T1", "T2", "T3")
    }
    regimes: dict[Regime, tuple[str, ...]] = {}
    tolerances: dict[Regime, float] = {}
    for name, spec in raw["regimes"].items():
        regimes[name] = tuple(spec["columns"])
        if spec.get("compare") == "relative":
            tolerances[name] = float(spec["tolerance"])

    attrs = {
        name: SourceAttribute(
            name=name,
            levels=int(spec["levels"]),
            encoding=spec["encoding"],
            outputs=tuple(spec["outputs"]),
            selected=tuple(spec["selected"]),
        )
        for name, spec in raw["source_attributes"].items()
    }

    fs = SelectedFeatureSet(
        id=raw["id"],
        split_allowed=raw["split_allowed"],
        expected_column_count=int(raw["expected_column_count"]),
        tiers=tiers,
        regimes=regimes,
        tolerances=tolerances,
        source_attributes=attrs,
        intermediate_only=frozenset(raw.get("intermediate_only", ())),
    )
    _validate(fs)
    return fs


def _validate(fs: SelectedFeatureSet) -> None:
    """Bat bien cua artifact — fail som con hon de solver chay tren scope sai."""
    # Tier khong duoc chong lan
    seen: set[str] = set()
    for tier, cols in fs.tiers.items():
        dup = seen & set(cols)
        if dup:
            raise ValueError(f"cot xuat hien o nhieu tier: {sorted(dup)}")
        seen |= set(cols)

    # Moi cot phai co dung mot regime
    regime_cols = {c for cols in fs.regimes.values() for c in cols}
    if regime_cols != fs.column_set:
        raise ValueError(
            f"regime khong phu het {len(fs.column_set)} cot: "
            f"thieu={sorted(fs.column_set - regime_cols)} "
            f"thua={sorted(regime_cols - fs.column_set)}"
        )

    # §5: `selected` cua moi source attribute phai nam trong T2
    t2 = set(fs.tiers["T2"])
    for attr in fs.source_attributes.values():
        stray = set(attr.selected) - t2
        if stray:
            raise ValueError(f"{attr.name}: cot selected khong thuoc T2: {sorted(stray)}")

    # §1.2: cot trung gian KHONG duoc lot vao target contract
    leak = fs.intermediate_only & fs.column_set
    if leak:
        raise ValueError(
            f"cot intermediate_only lot vao {len(fs.columns)} cot muc tieu: {sorted(leak)}"
        )

    # §5 / G-1: MOI cot cua group phai duoc khai bao — selected HOAC
    # intermediate_only. Neu mot muc bi bo quen o ca hai cho, SQL se dung
    # thieu muc va bat bien one-hot `sum(outputs) == 1` vo mot cach am tham.
    for attr in fs.source_attributes.values():
        unaccounted = set(attr.outputs) - fs.column_set - fs.intermediate_only
        if unaccounted:
            raise ValueError(
                f"{attr.name}: cot cua group khong duoc khai bao o dau ca: "
                f"{sorted(unaccounted)}. Phai nam trong T2 hoac intermediate_only, "
                "neu khong bat bien one-hot cua group se vo."
            )

    # 🚫 INVARIANT 1 — con so do CHINH artifact chot, khong phai code
    if len(fs.columns) != fs.expected_column_count:
        raise ValueError(
            f"{fs.id}: scope phai la DUNG {fs.expected_column_count} cot, "
            f"dang co {len(fs.columns)}"
        )
