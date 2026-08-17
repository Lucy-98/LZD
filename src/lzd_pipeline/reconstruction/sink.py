"""Land ket qua Track A vao ha tang that: MinIO lake · Postgres · DuckDB.

`track_a_batch` sinh ra mot THU MUC ARTIFACT (`.tmp/...` hoac bat ky dau).
Module nay dua thu muc do vao ba noi, moi noi mot vai tro khac han:

    MinIO    raw/events_v2/*.parquet    LAKE — event tho, immutable, dbt doc
    Postgres biz.*                      CONTROL PLANE — rang buoc + tamper trigger
    DuckDB   biz.*                      WAREHOUSE — ban sao doc de dbt join nhanh

★ THU TU KHONG DAO DUOC
--------------------------------------------------------------------------
    1. Postgres   <- artifact      (constraint TU CHOI du lieu sai)
    2. DuckDB     <- artifact      (chi sau khi (1) da chap nhan)
    3. MinIO      <- artifact      (event tho)

Postgres di TRUOC vi no la noi duy nhat co rang buoc that:
`ck_scope_55`, `ck_no_label`, FK toi `reconstruction_target`, va trigger chan
UPDATE/DELETE. Nap DuckDB truoc roi Postgres do se de lai warehouse chua du
lieu ma control plane da tu choi — trang thai khong the dieu tra duoc.

★ DUCKDB NAP TU ARTIFACT, KHONG PHAI TU POSTGRES
--------------------------------------------------------------------------
Nap tu Postgres se can extension `postgres` va ATTACH (ten 3 phan, phai sua
khai bao source cua dbt — xem `warehouse.py`). Nap tu artifact thi don gian
hon, DOI LAI phai chap nhan rui ro hai ban trot lech nhau. Rui ro do duoc dong
bang `verify_duckdb_matches_artifact()`: so dong tung bang phai khop artifact.

🚫 KHONG goi module nay tu `dag_00`. Bootstrap ket thuc o shell RONG (§12.2).
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from lzd_pipeline.reconstruction.feature_set import SelectedFeatureSet, load_feature_set
from lzd_pipeline.reconstruction.warehouse import BIZ_RELATIONS, BIZ_SCHEMA

if TYPE_CHECKING:  # pragma: no cover
    import duckdb

#: artifact file  ->  relation. Thu tu QUAN TRONG: `reconstruction_target` phai
#: di dau vi `customer_attribute` / `passthrough_source` co FK toi no.
BIZ_LOAD_ORDER: tuple[tuple[str, str], ...] = (
    ("biz_reconstruction_target.csv", "reconstruction_target"),
    ("biz_reconstruction_boundary.csv", "reconstruction_boundary"),
    ("biz_customer_attribute.csv", "customer_attribute"),
    ("biz_encoding_map.csv", "encoding_map"),
    ("biz_onehot_layout.csv", "onehot_layout"),
    ("biz_passthrough_source.csv", "passthrough_source"),
)

#: ⚠️ HAI KHO CHUA, HAI TAP RELATION KHAC NHAU — khong phai su bat can.
#:
#:   Postgres  bo `reconstruction_boundary`: o do no la VIEW
#:             (`v_reconstruction_boundary`) dan xuat tu `reconstruction_target`.
#:             COPY vao view se do.
#:
#:   DuckDB    bo `reconstruction_target`: no chua `payload` = ca 55 gia tri
#:             feature. Dua vao warehouse la mo duong cho Track B nhin trom
#:             target (INVARIANT 4, SPEC §3.4-1). Trong DuckDB `boundary` la
#:             bang phang, nap thang tu artifact.
POSTGRES_SKIP = frozenset({"reconstruction_boundary"})
DUCKDB_SKIP = frozenset({"reconstruction_target"})

#: File event tho -> lake.
EVENTS_FILE = "raw_events_v2.csv"


class SinkError(RuntimeError):
    """Land that bai. 🚫 KHONG nuot — du lieu nua voi kho dieu tra hon la do han."""


def _require(path: Path) -> Path:
    if not path.exists():
        raise SinkError(
            f"thieu artifact {path.name} trong {path.parent}. "
            "Chay `python -m lzd_pipeline.reconstruction.track_a_batch` truoc."
        )
    return path


def _row_count(path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as fh:
        return sum(1 for _ in csv.reader(fh)) - 1      # tru header


def artifact_row_counts(output_dir: Path) -> dict[str, int]:
    """So dong cua tung file artifact — moc de doi chieu sau khi nap."""
    counts = {rel: _row_count(_require(output_dir / name))
              for name, rel in BIZ_LOAD_ORDER}
    counts["events"] = _row_count(_require(output_dir / EVENTS_FILE))
    return counts


# ===========================================================================
# BUOC 2 · MinIO — event tho
# ===========================================================================
def lake_uri(*parts: str) -> str:
    from lzd_pipeline.common.config import get_settings

    return "/".join([get_settings().lake_root.rstrip("/"), *parts])


def publish_events_to_lake(
    output_dir: Path,
    dt: str,
    *,
    con: "duckdb.DuckDBPyConnection | None" = None,
    target_uri: str | None = None,
) -> dict[str, Any]:
    """`raw_events_v2.csv` -> parquet snappy tren MinIO.

    Dung DuckDB COPY nen khong nap het vao RAM — full train sinh ~17.9M event.

    `event_ts` / `observation_ts` giu nguyen kieu DOUBLE (epoch giay) dung nhu
    `stg_events_v2.sql` mong doi. 🚫 Khong tu y doi sang TIMESTAMP o day: bien
    PIT cua `feat_cfs_*` so sanh nghiem ngat `<`, doi kieu se lam lech mot
    event o bien va chi lo o mot ti le nho user.
    """
    source = _require(output_dir / EVENTS_FILE)
    target = target_uri or lake_uri("raw", "events_v2", f"dt={dt}", "events.parquet")

    sql = f"""
    COPY (SELECT * FROM read_csv_auto('{source.as_posix()}', header=true))
    TO '{target}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """

    def _run(c: "duckdb.DuckDBPyConnection") -> int:
        c.execute(sql)
        return int(c.execute(
            f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0])

    if con is not None:
        rows = _run(con)
    else:
        from lzd_pipeline.common.clients import duckdb_writer

        with duckdb_writer() as owned:
            rows = _run(owned)

    expected = _row_count(source)
    if rows != expected:
        raise SinkError(
            f"lake nhan {rows} event nhung artifact co {expected} — nap thieu"
        )
    return {"target": target, "rows": rows, "dt": dt}


#: Track B ghi sang PREFIX RIENG, khong dung chung `raw/events_v2/` voi
#: Track A. Xem `publish_track_b_to_lake()`.
TRACK_B_PREFIX = ("raw", "track_b_future")
TRACK_B_FILE = "track_b_events.csv"


def publish_track_b_to_lake(
    output_dir: Path,
    dt: str,
    *,
    con: "duckdb.DuckDBPyConnection | None" = None,
    target_uri: str | None = None,
) -> dict[str, Any]:
    """`track_b_events.csv` -> parquet tren MinIO, PREFIX RIENG.

    ★ VI SAO KHONG DUNG CHUNG `raw/events_v2/` VOI TRACK A
    ----------------------------------------------------------------------
    Hai Track khac nhau ve LOAI SU THAT, khong phai ve nhan:

        Track A  tai dung hanh vi DA XAY RA tu feature co that
                 -> du lieu quan sat chinh dang
        Track B  hanh vi do `RuleBasedBehaviour` BIA RA tu CustomerState(T0)
                 -> train tren no la day model hoc lai luat cua chinh no

    Neu hai loai nam chung mot prefix, moi dbt model moi ai do viet deu phai
    nho them `where source_type != 'SYNTHETIC'`. Quen mot lan la nhiem, ma
    nhiem kieu nay KHONG CO TRIEU CHUNG: metric van dep, co khi con dep hon,
    vi model dang duoc cham tren chinh hanh vi ma luat cua no sinh ra.

    Prefix rieng thi khong co dong loc nao de quen. Khong model dbt production
    nao doc `raw/track_b_future/` — muon dung phai khai bao source moi, tuc la
    mot hanh dong co y thuc.

    🚫 Cung vi vay: KHONG replay Track B vao topic `app.user.events.v1`. Topic
       do chay thang vao `raw/app_events` -> `feat_user_realtime_pit` va Redis
       overlay. Muon thu tai thi dung topic rieng.
    """
    source = _require(output_dir / TRACK_B_FILE)
    target = target_uri or lake_uri(*TRACK_B_PREFIX, f"dt={dt}", "events.parquet")

    sql = f"""
    COPY (SELECT * FROM read_csv_auto('{source.as_posix()}', header=true))
    TO '{target}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """

    def _run(c: "duckdb.DuckDBPyConnection") -> int:
        c.execute(sql)
        return int(c.execute(
            f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0])

    if con is not None:
        rows = _run(con)
    else:
        from lzd_pipeline.common.clients import duckdb_writer

        with duckdb_writer() as owned:
            rows = _run(owned)

    expected = _row_count(source)
    if rows != expected:
        raise SinkError(
            f"lake nhan {rows} event Track B nhung artifact co {expected} — nap thieu"
        )
    return {"target": target, "rows": rows, "dt": dt, "track": "B"}


# ===========================================================================
# BUOC 3a · Postgres — control plane
# ===========================================================================
def _csv_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as fh:
        return next(csv.reader(fh))


def publish_biz_to_postgres(output_dir: Path, *, dsn: str | None = None) -> dict[str, int]:
    """Nap 6 file artifact vao `biz.*` cua Postgres, dung THU TU FK.

    Dung `COPY ... FROM STDIN CSV HEADER` — nhanh hon INSERT tung dong nhieu
    lan, va quan trong hon: no chay trong MOT transaction, nen constraint tu
    choi la KHONG CO GI duoc ghi. Nap nua voi con te hon nap that bai.

    🚫 KHONG `ON CONFLICT DO NOTHING`. `reconstruction_target` la IMMUTABLE
       (SPEC §10) — trung target_id nghia la ai do dang chay lai voi cung
       reference_ts nhung du lieu khac, do la loi can dieu tra, khong phai
       chuyen de bo qua.
    """
    import psycopg2       # lazy: chi container moi co

    from lzd_pipeline.common.config import get_settings

    paths = [(_require(output_dir / name), rel) for name, rel in BIZ_LOAD_ORDER
             if rel not in POSTGRES_SKIP]
    loaded: dict[str, int] = {}

    conn = psycopg2.connect(dsn or get_settings().postgres.dsn)
    try:
        with conn.cursor() as cur:
            for path, relation in paths:
                cols = ", ".join(_csv_header(path))
                with path.open(newline="", encoding="utf-8") as fh:
                    cur.copy_expert(
                        f"COPY {BIZ_SCHEMA}.{relation} ({cols}) "
                        "FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                        fh,
                    )
                loaded[relation] = _row_count(path)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return loaded


# ===========================================================================
# BUOC 3b · DuckDB — ban sao doc cho dbt
# ===========================================================================
def load_biz_into_duckdb(
    con: "duckdb.DuckDBPyConnection",
    output_dir: Path,
    *,
    fs: SelectedFeatureSet | None = None,
) -> dict[str, int]:
    """Nap 5 relation ma dbt doc vao shell `biz.*` cua DuckDB.

    `reconstruction_target` KHONG duoc nap: khong model dbt nao doc no, va no
    chua `payload` — tuc toan bo 55 gia tri feature. Dua no vao warehouse la mo
    duong cho Track B "nhin trom" target (INVARIANT 4, SPEC §3.4-1).
    """
    fs = fs or load_feature_set()
    loaded: dict[str, int] = {}

    for name, relation in BIZ_LOAD_ORDER:
        if relation in DUCKDB_SKIP:
            continue                      # ★ co y bo qua — xem DUCKDB_SKIP
        path = _require(output_dir / name)
        cols = _csv_header(path)
        con.execute(f"DELETE FROM {BIZ_SCHEMA}.{relation}")     # nap lai la thay the
        con.execute(
            f"INSERT INTO {BIZ_SCHEMA}.{relation} ({', '.join(cols)}) "
            f"SELECT {', '.join(cols)} FROM read_csv_auto('{path.as_posix()}', header=true)"
        )
        loaded[relation] = int(con.execute(
            f"SELECT count(*) FROM {BIZ_SCHEMA}.{relation}").fetchone()[0])
    return loaded


def verify_duckdb_matches_artifact(
    con: "duckdb.DuckDBPyConnection", output_dir: Path
) -> dict[str, int]:
    """DuckDB va artifact phai khop tung bang.

    Day la thu dong lai rui ro cua viec nap DuckDB tu artifact thay vi tu
    Postgres (xem docstring module). Khong co buoc nay thi hai ban co the lech
    ma khong ai biet cho toi luc Gate A do vi ly do khong lien quan.
    """
    expected = artifact_row_counts(output_dir)
    actual: dict[str, int] = {}
    mismatch: list[str] = []
    for relation in BIZ_RELATIONS:
        n = int(con.execute(
            f"SELECT count(*) FROM {BIZ_SCHEMA}.{relation}").fetchone()[0])
        actual[relation] = n
        if n != expected[relation]:
            mismatch.append(f"{relation}: duckdb={n} artifact={expected[relation]}")
    if mismatch:
        raise SinkError("DuckDB lech artifact — " + "; ".join(mismatch))
    return actual


# ===========================================================================
# Dieu phoi
# ===========================================================================
def land_track_a(
    output_dir: Path,
    dt: str,
    *,
    con: "duckdb.DuckDBPyConnection",
    to_postgres: bool = True,
    to_lake: bool = True,
) -> dict[str, Any]:
    """Postgres -> DuckDB -> MinIO. Thu tu nay khong dao duoc (xem module doc).

    `to_postgres` / `to_lake` chi de tat trong test va dry-run tren host khong
    co stack. 🚫 Tat chung o production nghia la warehouse co du lieu ma control
    plane chua bao gio duyet.
    """
    report: dict[str, Any] = {"output_dir": str(output_dir), "dt": dt}

    if to_postgres:
        report["postgres"] = publish_biz_to_postgres(output_dir)
    else:
        report["postgres"] = "SKIPPED"

    report["duckdb"] = load_biz_into_duckdb(con, output_dir)
    report["duckdb_verified"] = verify_duckdb_matches_artifact(con, output_dir)

    if to_lake:
        report["lake"] = publish_events_to_lake(output_dir, dt, con=con)
    else:
        report["lake"] = "SKIPPED"

    return report


def load_events_into_duckdb(
    con: "duckdb.DuckDBPyConnection", output_dir: Path, *, relation: str = "raw.events_v2"
) -> int:
    """Nap event vao `raw.events_v2` cua DuckDB tu artifact.

    Dung khi chay tren host khong co MinIO. O production, `raw.events_v2` giai
    bang `external_location` tro vao parquet tren lake — khong can ham nay.
    """
    path = _require(output_dir / EVENTS_FILE)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {relation.split('.')[0]}")
    con.execute(
        f"CREATE OR REPLACE TABLE {relation} AS "
        f"SELECT * FROM read_csv_auto('{path.as_posix()}', header=true)"
    )
    return int(con.execute(f"SELECT count(*) FROM {relation}").fetchone()[0])


def main() -> None:
    """CLI: xem truoc ke hoach land, hoac land that.

        python -m lzd_pipeline.reconstruction.sink --output-dir .tmp/track_a
        python -m lzd_pipeline.reconstruction.sink --output-dir .tmp/track_a --land
    """
    import argparse
    import json
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Land ket qua Track A vao ha tang")
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="thu muc artifact do track_a_batch sinh ra")
    parser.add_argument("--dt", default="2026-08-05", help="partition dt tren lake")
    parser.add_argument("--land", action="store_true",
                        help="GHI THAT vao Postgres + DuckDB + MinIO. "
                             "Khong co co nay thi chi in ke hoach.")
    parser.add_argument("--no-postgres", action="store_true")
    parser.add_argument("--no-lake", action="store_true")
    args = parser.parse_args()

    if not args.land:
        print(json.dumps({
            "mode": "DRY-RUN (them --land de ghi that)",
            "artifact_rows": artifact_row_counts(args.output_dir),
            "postgres_plan": [
                {k: v for k, v in step.items() if k != "columns"}
                for step in postgres_load_plan(args.output_dir)
            ],
            "duckdb_relations": [
                f"{BIZ_SCHEMA}.{r}" for _, r in BIZ_LOAD_ORDER if r not in DUCKDB_SKIP
            ],
            "lake_target": lake_uri("raw", "events_v2", f"dt={args.dt}", "events.parquet"),
        }, indent=2, ensure_ascii=False))
        return

    from lzd_pipeline.common.clients import duckdb_writer

    with duckdb_writer() as con:
        report = land_track_a(
            args.output_dir, args.dt, con=con,
            to_postgres=not args.no_postgres, to_lake=not args.no_lake,
        )
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))


def postgres_load_plan(output_dir: Path) -> list[dict[str, Any]]:
    """Mo ta buoc nap Postgres ma KHONG chay — de kiem hop dong cot khi khong
    co Postgres, va de in ra trong dry-run."""
    plan: list[dict[str, Any]] = []
    for name, relation in BIZ_LOAD_ORDER:
        if relation in POSTGRES_SKIP:
            continue                      # VIEW, khong COPY vao duoc
        path = _require(output_dir / name)
        plan.append({
            "file": name,
            "relation": f"{BIZ_SCHEMA}.{relation}",
            "columns": _csv_header(path),
            "rows": _row_count(path),
        })
    return plan


if __name__ == "__main__":
    main()
