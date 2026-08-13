"""Encoding contract — RECONSTRUCTION_SPEC.md §8.

Property BAT BUOC, kiem tren TOAN domain (khong phai sample):

    forall x in observed_domain :  encode(decode(x)) == x
    forall s in level_domain    :  decode(encode(s)) == s

🚫 BAY §8.3: alphabet(f37) la TAP CON hoan toan cua alphabet(f38) (64/64 gia tri),
   nhung card(tuple(f37,f38)) = 1133 > 241 => HAI bien doc lap.
   => MOT ham encode() dung chung cho ca hai se lam decode(v) MAP MO.
   => encode/decode BAT BUOC co khoa thuoc tinh.
"""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Mapping

from lzd_pipeline.reconstruction.canonical import float_repr


class EncodingError(ValueError):
    """Gia tri khong tra duoc trong map => FAIL CUNG (§8, C-5). Khong fallback."""


@dataclass(frozen=True)
class ValueEncoding:
    """Song anh `level_id <-> tuple gia tri` cho MOT thuoc tinh.

    Dung cho `encoding_map_515` (4 cot), `encoding_map_64`, `encoding_map_241`.
    Ban do duoc FIT tu du lieu quan sat — giong het cach mot encoder production
    duoc fit tren training data roi luu artifact.

    Khoa tra cuu dung `float_repr` chu khong dung float thuan: da do duoc
    `card(DISTINCT value) == card(DISTINCT CAST(value AS VARCHAR))` cho ca 6 cot
    => khoa text an toan va tranh moi mo ho -0.0 / bit cuoi.
    """

    attr: str
    columns: tuple[str, ...]
    _to_level: Mapping[tuple[str, ...], int]
    _to_value: Mapping[int, tuple[float, ...]]

    @property
    def levels(self) -> int:
        return len(self._to_value)

    def decode(self, values: Mapping[str, float]) -> int:
        key = tuple(float_repr(values[c]) for c in self.columns)
        try:
            return self._to_level[key]
        except KeyError:
            raise EncodingError(
                f"{self.attr}: gia tri khong co trong encoding map: "
                f"{dict(zip(self.columns, key))}. "
                "KHONG fallback — map fit sai hoac du lieu vao khac du lieu da audit."
            ) from None

    def encode(self, level: int) -> dict[str, float]:
        try:
            vals = self._to_value[level]
        except KeyError:
            raise EncodingError(f"{self.attr}: level {level} ngoai mien [0,{self.levels})") from None
        return dict(zip(self.columns, vals))


@dataclass(frozen=True)
class OneHotEncoding:
    """One-hot cho mot group.

    §5 / G-1: `columns` la TOAN BO cot cua group, ke ca cot khong duoc chon.
    Chi dung 3/10 muc se lam vo bat bien `sum(f43..f52) == 1`.
    """

    attr: str
    columns: tuple[str, ...]

    @property
    def levels(self) -> int:
        return len(self.columns)

    def decode(self, values: Mapping[str, float]) -> int:
        hot = [i for i, c in enumerate(self.columns) if values[c] == 1.0]
        if len(hot) != 1:
            raise EncodingError(
                f"{self.attr}: one-hot invariant vo — sum={len(hot)}, phai bang 1. "
                f"({dict((c, values[c]) for c in self.columns)})"
            )
        return hot[0]

    def encode(self, level: int) -> dict[str, float]:
        if not 0 <= level < self.levels:
            raise EncodingError(f"{self.attr}: level {level} ngoai mien [0,{self.levels})")
        return {c: (1.0 if i == level else 0.0) for i, c in enumerate(self.columns)}


Encoding = ValueEncoding | OneHotEncoding


def fit_value_encoding(
    attr: str, columns: Sequence[str], rows: Iterable[Mapping[str, float]]
) -> ValueEncoding:
    """Fit ban do tu du lieu quan sat.

    Level id gan theo THU TU SAP XEP cua khoa — tat dinh, khong phu thuoc thu tu
    duyet `rows`. Neu gan theo thu tu gap dau tien thi hai lan fit tren cung tap
    du lieu nhung khac thu tu dong se cho map khac nhau.
    """
    cols = tuple(columns)
    seen: dict[tuple[str, ...], tuple[float, ...]] = {}
    for row in rows:
        key = tuple(float_repr(row[c]) for c in cols)
        if key not in seen:
            seen[key] = tuple(float(row[c]) for c in cols)

    ordered = sorted(seen)
    to_level = {key: i for i, key in enumerate(ordered)}
    to_value = {i: seen[key] for i, key in enumerate(ordered)}
    return ValueEncoding(attr=attr, columns=cols, _to_level=to_level, _to_value=to_value)


class EncodingRegistry:
    """Tra cuu encoding THEO THUOC TINH — §8.3.

    🚫 KHONG co API `decode(value)` khong khoa. Chu ky ham la noi thuc thi
       rang buoc nay, khong phai comment.
    """

    def __init__(self, encodings: Mapping[str, Encoding]) -> None:
        self._by_attr = dict(encodings)

    def __contains__(self, attr: object) -> bool:
        return attr in self._by_attr

    @property
    def attrs(self) -> tuple[str, ...]:
        return tuple(sorted(self._by_attr))

    def get(self, attr: str) -> Encoding:
        try:
            return self._by_attr[attr]
        except KeyError:
            raise EncodingError(
                f"khong co encoding cho thuoc tinh {attr!r}. "
                f"Co: {self.attrs}"
            ) from None

    def decode(self, attr: str, values: Mapping[str, float]) -> int:
        return self.get(attr).decode(values)

    def encode(self, attr: str, level: int) -> dict[str, float]:
        return self.get(attr).encode(level)

    # -- §8.1 property, kiem TOAN domain ------------------------------------
    def assert_bijective(self, attr: str) -> None:
        """`decode(encode(s)) == s` cho MOI level. Khong sample."""
        enc = self.get(attr)
        for level in range(enc.levels):
            back = enc.decode(enc.encode(level))
            if back != level:
                raise EncodingError(
                    f"{attr}: khong song anh — encode({level}) -> decode -> {back}"
                )

    def assert_round_trip(self, attr: str, rows: Iterable[Mapping[str, float]]) -> None:
        """`encode(decode(x)) == x` cho MOI gia tri quan sat."""
        enc = self.get(attr)
        for row in rows:
            level = enc.decode(row)
            got = enc.encode(level)
            for c, v in got.items():
                if float_repr(v) != float_repr(row[c]):
                    raise EncodingError(
                        f"{attr}: round-trip lech o cot {c}: "
                        f"goc={float_repr(row[c])} tai_sinh={float_repr(v)}"
                    )
