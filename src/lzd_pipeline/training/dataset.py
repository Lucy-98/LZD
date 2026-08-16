"""Doc training dataset tu DuckDB (mart do dbt build).

Phan nay DA XONG - teammate lam model chi can goi `load_training_frame()`
la co dung bo feature ma luc serving se doc tu Redis (cung feature_spec.yml),
nen khong lo training/serving skew.
"""
from __future__ import annotations

from typing import Any

from lzd_pipeline.common.clients import duckdb_conn
from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.features.spec import FeatureSpec, load_feature_spec

log = get_logger(__name__)


def feature_columns(spec: FeatureSpec | None = None) -> list[str]:
    """Dung THU TU NAY khi build ma tran X luc train VA luc serve."""
    spec = spec or load_feature_spec()
    return spec.all_names


#: Bang danh gia DONG BANG. Tach khoi `training_table` co y — xem
#: `dbt/models/marts/eval_holdout.sql`.
HOLDOUT_TABLE = "marts.eval_holdout"


def load_training_frame(
    split: str | None = None,
    dt: str | None = None,
    dt_from: str | None = None,
    dt_to: str | None = None,
    limit: int | None = None,
    spec: FeatureSpec | None = None,
):
    """Tra ve pandas.DataFrame gom: user_id, label, is_treat, <tat ca feature>.

    `dt_from`/`dt_to` la CUA SO tuong minh. Goi ham nay khong kem cua so se
    doc ca bang — ma noi dung bang phu thuoc vao viec dbt duoc goi the nao lan
    cuoi, nen hai lan train co the thay hai luong du lieu khac han. Duong
    huan luyen PHAI noi ro no doc gi (xem `train.run_training`).
    """
    spec = spec or load_feature_spec()
    table = spec.offline["training_table"]
    cols = [spec.entity_key, spec.label_column, spec.treatment_column] + feature_columns(spec)
    col_sql = ", ".join(f'"{c}"' for c in cols)

    where = []
    if split:
        where.append(f"split = '{split}'")
    if dt:
        where.append(f"dt = DATE '{dt}'")
    if dt_from:
        where.append(f"dt >= DATE '{dt_from}'")
    if dt_to:
        where.append(f"dt <= DATE '{dt_to}'")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    limit_sql = f"LIMIT {limit}" if limit else ""

    sql = f"SELECT {col_sql} FROM {table} {where_sql} {limit_sql}"
    log.info("doc training dataset",
             extra={"event": "load_training_frame", "table": table, "split": split,
                    "dt": dt, "dt_from": dt_from, "dt_to": dt_to})
    with duckdb_conn(read_only=True) as con:
        return con.execute(sql).df()


def load_eval_holdout(
    limit: int | None = None, spec: FeatureSpec | None = None
) -> tuple[Any, str]:
    """Tra ve `(frame, holdout_version)` cua tap danh gia dong bang.

    🚫 KHONG nhan tham so cua so thoi gian. Tap nay chot o `holdout_dt` khai
       trong `dbt_project.yml`; cho phep loc theo ngay o day la mo lai dung
       cai khe ma no sinh ra de bit: `promote.py` so `qini` giua cac run, va
       phep so do chi co nghia neu moi run do tren CUNG mot tap.
    """
    spec = spec or load_feature_spec()
    cols = [spec.entity_key, spec.label_column, spec.treatment_column] + feature_columns(spec)
    col_sql = ", ".join(f'"{c}"' for c in cols)
    limit_sql = f"LIMIT {limit}" if limit else ""

    with duckdb_conn(read_only=True) as con:
        frame = con.execute(
            f"SELECT {col_sql} FROM {HOLDOUT_TABLE} {limit_sql}"
        ).df()
        versions = [
            r[0] for r in
            con.execute(f"SELECT DISTINCT holdout_version FROM {HOLDOUT_TABLE}").fetchall()
        ]

    if len(versions) != 1:
        raise ValueError(
            f"{HOLDOUT_TABLE} phai co dung MOT holdout_version, dang co {versions}. "
            "Hai phien ban lan trong cung mot bang nghia la khong con biet metric "
            "do tren tap nao."
        )

    log.info("doc eval holdout",
             extra={"event": "load_eval_holdout", "table": HOLDOUT_TABLE,
                    "holdout_version": versions[0], "n_rows": len(frame)})
    return frame, str(versions[0])


def training_window(dt: str, weeks: int) -> tuple[str, str]:
    """Cua so `[dt_from, dt_to]` ket thuc o `dt`, dai `weeks` tuan."""
    from datetime import date, timedelta

    end = date.fromisoformat(dt)
    return (end - timedelta(weeks=weeks)).isoformat(), end.isoformat()


def split_xyt(frame, spec: FeatureSpec | None = None) -> tuple[Any, Any, Any]:
    """Tach (X, y, treatment) - dau vao chuan cho moi thu vien uplift."""
    spec = spec or load_feature_spec()
    x = frame[feature_columns(spec)]
    y = frame[spec.label_column]
    treatment = frame[spec.treatment_column]
    return x, y, treatment


def dataset_stats(frame, spec: FeatureSpec | None = None) -> dict[str, float]:
    """So lieu dua vao MLflow + Grafana de theo doi drift giua cac lan train."""
    spec = spec or load_feature_spec()
    n = len(frame)
    treated = frame[spec.treatment_column].sum()
    return {
        "n_rows": float(n),
        "n_features": float(len(feature_columns(spec))),
        "treatment_ratio": float(treated / n) if n else 0.0,
        "conversion_rate": float(frame[spec.label_column].mean()) if n else 0.0,
        "conversion_rate_treated": float(
            frame.loc[frame[spec.treatment_column] == 1, spec.label_column].mean()
        ) if treated else 0.0,
        "conversion_rate_control": float(
            frame.loc[frame[spec.treatment_column] == 0, spec.label_column].mean()
        ) if n - treated else 0.0,
    }
