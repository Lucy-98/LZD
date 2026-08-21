"""Dung vector dau vao model cho duong quyet dinh.

★ DUONG DUY NHAT DUNG VECTOR
--------------------------------------------------------------------------
`/decide` va `/decide/batch` deu goi `build_model_row()`. Truoc day chung
dung hai duong rieng va cho ra HAI SCORE KHAC NHAU cho cung mot user:

    /decide        -0.128802
    /decide/batch  +0.003643

Mot khac biet khong the giai thich tu ben ngoai. Mot duong thi chung khong
the lech nua.

★ HAI TANG MAC DINH CHONG LEN NHAU — day la goc cua bug tren
--------------------------------------------------------------------------
    spec.merge()             dien 0.0      (feature_spec.yml)
    feature_contract._num()  dien TRUNG VI (hop dong model)

0.0 la mot gia tri CO MAT, nen `_num()` coi do la du lieu that va khong con
co hoi dien trung vi. `[MEASURED]` 28/55 cot chung lech nhau:

    f1   spec 0.0  <->  trung vi 172.0
    f2   spec 0.0  <->  trung vi 166.0
    f30  spec 0.0  <->  trung vi 0.845

Dua 0.0 cho mot cot co trung vi 172 la day mau ra ngoai han mien huan luyen —
model van tra ve so, chi la so khong co nghia. Nen o day cot THIEU bi BO RA
KHOI dict, de hop dong dien trung vi dung nhu no duoc thiet ke.

🚫 Dung `spec.merge()` cho DQ/validate, KHONG dung cho duong suy luan.

★ MODULE NAY KHONG IMPORT FASTAPI
--------------------------------------------------------------------------
Co y: `requirements-dev.txt` khong cai fastapi, nen logic nam trong `app.py`
se khong test duoc o moi truong dev chuan. Loi HTTP do `app.py` dich tu
`ContextError`.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence


class ContextError(ValueError):
    """`context` chua khoa khong co trong `feature_spec.yml`.

    `app.py` dich thanh HTTP 422. 🚫 KHONG bo qua im lang: khoa sai chinh ta
    duoc nhan voi HTTP 200 roi bien mat nghia la client tuong da gui tin hieu
    ma thuc te khong co gi toi model — sai kieu do khong bao gio lo ra tu
    phia goi.
    """

    def __init__(self, unknown_keys: Sequence[str]) -> None:
        self.unknown_keys = list(unknown_keys)
        super().__init__(f"khoa khong co trong feature_spec.yml: {self.unknown_keys}")


def check_context(spec, context: Mapping[str, Any] | None) -> None:
    if not context:
        return
    unknown = sorted(set(context) - set(spec.all_names))
    if unknown:
        raise ContextError(unknown)


def build_model_row(
    spec,
    model_feature_order: Iterable[str],
    batch_raw: Mapping[str, Any],
    realtime: Mapping[str, Any],
    context: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], int, int, int]:
    """Tra ve `(row, missing, supplied, realtime_applied)`.

    Thu tu uu tien: context > realtime > batch (moi hon thang cu).

    `realtime_applied` dem tin hieu realtime THUC SU vao duoc model. Bang 0
    KHONG co nghia la client gui thieu — model 76 cot cua notebook khong co o
    nao cho `rt_*`, nen realtime khong bao gio vao duoc no. Chi model do
    `train.py` huan luyen (62 cot, 7 cot rt_*) moi dung den. Truong nay de
    cho su that do lo ra thay vi phai suy tu score.
    """
    context = context or {}
    row: dict[str, Any] = {}
    for f in spec.all_features:
        raw = context.get(f.name)
        if raw is None:
            raw = realtime.get(f.name)
        if raw is None:
            raw = batch_raw.get(f.name)
        if raw is None or raw == "":
            continue                      # de hop dong dien trung vi
        row[f.name] = f.cast(raw)

    wanted = set(model_feature_order)
    missing = sum(1 for name in wanted if name not in row)
    realtime_applied = sum(
        1 for name in row if name.startswith("rt_") and name in wanted
    )
    supplied = len(wanted) - missing
    return row, missing, supplied, realtime_applied
