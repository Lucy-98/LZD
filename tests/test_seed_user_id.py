"""`user_id` sinh tu `data_id` — bat bien chong va cham giua train va test.

★ Rui ro ma lop test nay canh: MAT DU LIEU AM THAM.

Cong thuc cu chi lay chu so trong `data_id`:

    'U' || lpad(regexp_extract(data_id, '\\d+'), 7, '0')

Hai file CSV danh so DOC LAP tu 0, nen `train_0` va `test_0` cung ra
`U0000000`. `stg_user_snapshot` khu trung theo `(user_id, dt)` giu ban
`feature_ts` moi nhat -> mot trong hai nguoi bi xoa.

`[MEASURED]` Hau qua tren may that:

    381,669 dong nap vao  ->  200,000 user_id
    train con 18,331 / 200,000   (mat 91.7%)
    model train tren phan sot lai: qini 0.000128 (notebook: 0.02324)

Khong co canh bao nao — DAG xanh, dbt xanh, chi co ket qua cuoi la sai. Do la
ly do bat bien nay phai duoc kiem bang test chu khong bang tri nho.
"""
from __future__ import annotations

import pytest

duckdb = pytest.importorskip("duckdb")

#: Doan SQL dang chay trong `seed_loader.seed_split()`. Giu dong bo bang tay —
#: test se do neu ai do doi cong thuc ma khong doi o day.
USER_ID_SQL = (
    "CASE WHEN lower(data_id) LIKE 'test%' THEN 'T' ELSE 'U' END "
    "|| lpad(regexp_extract(data_id, '\\d+'), 7, '0')"
)


def _user_id(con, data_id: str) -> str:
    return con.execute(
        f"SELECT {USER_ID_SQL} FROM (SELECT ? AS data_id)", [data_id]
    ).fetchone()[0]


@pytest.fixture(scope="module")
def con():
    with duckdb.connect(":memory:") as c:
        yield c


def test_train_va_test_cung_so_KHONG_duoc_trung_id(con):
    """Phep kiem trung tam. Truoc day ca hai deu ra U0000000."""
    assert _user_id(con, "train_0") != _user_id(con, "test_0")
    assert _user_id(con, "train_12345") != _user_id(con, "test_12345")


def test_train_giu_tien_to_U_de_join_duoc_voi_stream(con):
    """`event_producer` sinh `U{idx:07d}` — trung khong gian LA CO Y, de
    realtime overlay gap duoc feature batch. Doi tien to train la lam hong
    duong realtime."""
    assert _user_id(con, "train_0") == "U0000000"
    assert _user_id(con, "train_123") == "U0000123"
    assert _user_id(con, "train_1999999") == "U1999999"


def test_test_dung_tien_to_T(con):
    assert _user_id(con, "test_0") == "T0000000"
    assert _user_id(con, "test_123") == "T0000123"


def test_khong_co_va_cham_tren_dai_so_lon(con):
    """Sinh ID cho 2,000 cap train/test, khong duoc trung nhau doi nao."""
    rows = con.execute(f"""
        WITH ids AS (
            SELECT 'train_' || i AS data_id FROM range(2000) t(i)
            UNION ALL
            SELECT 'test_' || i FROM range(2000) t(i)
        )
        SELECT count(*) AS tong, count(DISTINCT {USER_ID_SQL}) AS rieng FROM ids
    """).fetchone()
    assert rows[0] == 4000
    assert rows[1] == 4000, "co user_id trung -> se mat du lieu khi dedup"
