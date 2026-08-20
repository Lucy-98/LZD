"""End-to-end runner — target -> E* -> FEATURE ENGINE THAT -> F' -> Gate A.

★ RANG BUOC QUAN TRONG NHAT: module nay KHONG viet lai logic feature.
  No render va THUC THI CHINH FILE SQL cua dbt trong DuckDB.

      🚫 CAM: reimplement counter/recency/categorical bang Python roi so
              => dang so dau ra cua mot ham voi CHINH NO (tautology §12)
      ✅ DUNG: doc dbt/models/**/*.sql, render Jinja, chay trong DuckDB

  Neu SQL sai, runner nay do. Neu runner nay xanh, SQL that su chay duoc.

⚠️ Day la HARNESS cho prototype/pilot, khong phai duong production. Production
   chay qua Airflow + dbt that. Nhung SQL thi la MOT.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

import duckdb
import jinja2

from lzd_pipeline.reconstruction import engine
from lzd_pipeline.reconstruction.feature_set import SelectedFeatureSet, load_feature_set
from lzd_pipeline.reconstruction.semantics import DecodedTarget, SemanticBranch

DBT_ROOT = Path(__file__).resolve().parents[3] / "dbt" / "models"

#: Model dbt -> bang DuckDB. `ref()` / `source()` deu tro vao day.
RELATIONS = {
    ("ref", "stg_events_v2"): "stg_events_v2",
    ("source", "raw", "events_v2"): "raw_events_v2",
    ("source", "biz", "reconstruction_boundary"): "biz_reconstruction_boundary",
    ("source", "biz", "customer_attribute"): "biz_customer_attribute",
    ("source", "biz", "encoding_map"): "biz_encoding_map",
    ("source", "biz", "onehot_layout"): "biz_onehot_layout",
    ("source", "biz", "passthrough_source"): "biz_passthrough_source",
}

DEFAULT_VARS = {
    "history_days": 30,
    "counter_window_days": 365,
    "reconstruction_encoding_version": "encoding_2026_08_v3",
}


# ===========================================================================
# Render dbt SQL — KHONG sua logic, chi thay ref/source/var
# ===========================================================================
def render_model(path: Path, dbt_vars: Mapping[str, Any] | None = None) -> str:
    v = {**DEFAULT_VARS, **(dbt_vars or {})}

    def _rel(kind: str, *parts: str) -> str:
        try:
            return RELATIONS[(kind, *parts)]
        except KeyError:
            raise KeyError(f"chua khai bao relation cho {kind}{parts}") from None

    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    tpl = env.from_string(path.read_text(encoding="utf-8"))
    return tpl.render(
        config=lambda **_: "",
        ref=lambda name: _rel("ref", name),
        source=lambda a, b: _rel("source", a, b),
        var=lambda name, default=None: v.get(name, default),
        this="__this__",
    )


# ===========================================================================
# Seed — dung tu ket qua solver, KHONG dung tu target
# ===========================================================================
@dataclass(frozen=True)
class RunInput:
    target_id: str
    decoded: DecodedTarget
    reference_ts: datetime
    attributes: Mapping[str, int]          # attr_name -> level_id
    passthrough: Mapping[str, float]       # 21 cot T3
    generation_run_id: str = "run-1"


def seed(
    con: duckdb.DuckDBPyConnection,
    runs: Sequence[RunInput],
    outcomes: Mapping[str, engine.SolveOutcome],
    *,
    encoding_rows: Sequence[tuple[str, int, str, float]],
    onehot_rows: Sequence[tuple[str, int, str]],
    encoding_version: str = "encoding_2026_08_v3",
) -> None:
    """Nap bang nguon. Event DUY NHAT den tu `outcomes` - tuc tu solver."""
    con.execute("SET TimeZone='UTC'")
    con.execute("""
        CREATE OR REPLACE TABLE raw_events_v2(
            event_id VARCHAR, customer_id VARCHAR, target_id VARCHAR,
            event_type VARCHAR, event_ts DOUBLE, observation_ts DOUBLE,
            source_type VARCHAR, generation_run_id VARCHAR,
            gen_reason VARCHAR, day_offset INTEGER, sub_index INTEGER, occurrence INTEGER)
    """)
    for r in runs:
        out = outcomes[r.target_id]
        for e in out.events:
            ts = e.event_ts.timestamp()
            con.execute(
                "INSERT INTO raw_events_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                [e.event_id, r.target_id, r.target_id, e.event_type, ts, ts,
                 "RECONSTRUCTED", r.generation_run_id,
                 # 🚫 Ba cot cuoi CO Y co mat o raw. `stg_events_v2` phai DROP
                 #    chung — neu no cho di qua, test TA-6 se do.
                 e.gen_reason, e.day_offset, e.sub_index, e.occurrence],
            )

    con.execute("CREATE OR REPLACE TABLE biz_reconstruction_boundary("
                "target_id VARCHAR, customer_id_hint VARCHAR, reference_ts TIMESTAMPTZ)")
    con.execute("CREATE OR REPLACE TABLE biz_customer_attribute("
                "target_id VARCHAR, attr_name VARCHAR, level_id INTEGER)")
    for r in runs:
        con.execute("INSERT INTO biz_reconstruction_boundary VALUES (?,?,?)",
                    [r.target_id, r.target_id, r.reference_ts])
        for a, lvl in r.attributes.items():
            con.execute("INSERT INTO biz_customer_attribute VALUES (?,?,?)",
                        [r.target_id, a, lvl])

    con.execute("CREATE OR REPLACE TABLE biz_encoding_map("
                "encoding_version VARCHAR, attr_name VARCHAR, level_id INTEGER, "
                "column_name VARCHAR, value DOUBLE)")
    con.executemany(
        "INSERT INTO biz_encoding_map VALUES (?,?,?,?,?)",
        [(encoding_version, *row) for row in encoding_rows],
    )

    con.execute("CREATE OR REPLACE TABLE biz_onehot_layout("
                "encoding_version VARCHAR, attr_name VARCHAR, level_index INTEGER, "
                "column_name VARCHAR)")
    con.executemany(
        "INSERT INTO biz_onehot_layout VALUES (?,?,?,?)",
        [(encoding_version, *row) for row in onehot_rows],
    )

    t3 = load_feature_set().tiers["T3"]
    cols = ", ".join(f"{c} DOUBLE" for c in t3)
    con.execute(f"CREATE OR REPLACE TABLE biz_passthrough_source(target_id VARCHAR, {cols})")
    ph = ",".join("?" * (len(t3) + 1))
    for r in runs:
        con.execute(f"INSERT INTO biz_passthrough_source VALUES ({ph})",
                    [r.target_id, *[r.passthrough[c] for c in t3]])


def build_marts(
    con: duckdb.DuckDBPyConnection,
    *,
    semantic_branch: str = "H1",
    history_days: int = 30,
    counter_window_days: int = 365,
    encoding_version: str = "encoding_2026_08_v3",
) -> None:
    """Chay CHINH SQL cua dbt, theo dung thu tu phu thuoc."""
    # Compatibility input for the legacy solver state; the active 30-feature
    # forward SQL does not contain f30 and therefore does not branch on it.
    _ = semantic_branch
    for rel, path in (
        ("stg_events_v2", DBT_ROOT / "staging" / "stg_events_v2.sql"),
        ("feat_cfs_counter", DBT_ROOT / "marts" / "feat_cfs_counter.sql"),
        ("feat_cfs_recency", DBT_ROOT / "marts" / "feat_cfs_recency.sql"),
        ("feat_cfs_categorical", DBT_ROOT / "marts" / "feat_cfs_categorical.sql"),
        ("feat_passthrough", DBT_ROOT / "marts" / "feat_passthrough.sql"),
        ):
        sql = render_model(path, {
            "history_days": history_days,
            "counter_window_days": counter_window_days,
            "reconstruction_encoding_version": encoding_version,
        })
        con.execute(f"CREATE OR REPLACE TABLE {rel} AS {sql}")


# ===========================================================================
# Gate A / A-T3 — so theo REGIME, KHONG so bang hash (§6.2)
# ===========================================================================
@dataclass(frozen=True)
class ColumnResult:
    column: str
    regime: str
    expected: float | None
    actual: float | None
    ok: bool


def compare(
    expected: Mapping[str, float], actual: Mapping[str, float], fs: SelectedFeatureSet
) -> list[ColumnResult]:
    out: list[ColumnResult] = []
    for col in fs.columns:
        regime = fs.regime_of(col)
        tol = fs.tolerance_of(col)
        e, a = expected.get(col), actual.get(col)
        if a is None or e is None:
            ok = False
        elif tol is None:
            ok = e == a                                   # LOG10 / REC / CAT / PASS
        else:
            ok = abs(a - e) <= tol * max(abs(e), 1.0)     # LN — dung sai TUONG DOI
        out.append(ColumnResult(col, regime, e, a, ok))
    return out


@dataclass(frozen=True)
class GateAReport:
    gate_a: tuple[ColumnResult, ...]        # 18 cot T1+T2
    gate_a_t3: tuple[ColumnResult, ...]     # 18 cot T3 — ti le RIENG

    @property
    def gate_a_pass(self) -> bool:
        return all(r.ok for r in self.gate_a)

    @property
    def gate_a_t3_pass(self) -> bool:
        return all(r.ok for r in self.gate_a_t3)

    def failures(self) -> tuple[ColumnResult, ...]:
        return tuple(r for r in (*self.gate_a, *self.gate_a_t3) if not r.ok)


def gate_a(
    expected: Mapping[str, float], actual: Mapping[str, float],
    fs: SelectedFeatureSet | None = None,
) -> GateAReport:
    """🚫 Gate A va Gate A-T3 bao cao ti le RIENG (§12).

    Gop chung lam ti le pass luon >= 50% nho copy — con so vo nghia.
    """
    fs = fs or load_feature_set()
    res = {r.column: r for r in compare(expected, actual, fs)}
    return GateAReport(
        gate_a=tuple(res[c] for c in fs.gate_a_columns),
        gate_a_t3=tuple(res[c] for c in fs.gate_a_t3_columns),
    )


def read_reconstructed(con: duckdb.DuckDBPyConnection, target_id: str) -> dict[str, float]:
    """Gom cac cot cua feature-set 30 cot tu cac mart."""
    out: dict[str, float] = {}
    for tbl in ("feat_cfs_counter", "feat_cfs_recency",
                "feat_cfs_categorical", "feat_passthrough"):
        cols = [c[0] for c in con.execute(f"DESCRIBE {tbl}").fetchall()]
        want = [c for c in cols if c.startswith("f") and c[1:].isdigit()]
        if not want:
            continue
        row = con.execute(
            f"SELECT {', '.join(want)} FROM {tbl} WHERE target_id = ?", [target_id]
        ).fetchone()
        if row:
            out.update({c: (float(v) if v is not None else None) for c, v in zip(want, row)})
    return out
