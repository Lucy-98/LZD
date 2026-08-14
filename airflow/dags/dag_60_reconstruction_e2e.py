"""DAG 60 - Track A reconstruction tren TRAIN SPLIT THAT + contract dry-run.

Hai che do, chon bang param `mode`:

    contract   solver + SQL dbt + Gate A..F + handoff + Track B, tren MOT target
               synthetic. Khong cham ha tang. Day la bai kiem HOP DONG.

    backfill   doc `data/full_trainset.csv` that, giai ma 55 feature, sinh event
               witness, roi LAND xuong ha tang:
                   Postgres biz.*  ->  DuckDB biz.*  ->  MinIO raw/events_v2
               Thu tu do khong dao duoc — xem `reconstruction/sink.py`.

⚠️ `backfill` GHI HA TANG THAT. `contract` thi khong.

Sau `backfill`, model reconstruction build bang:

    dbt run --select tag:reconstruction

(chung bi EXCLUDE khoi DAG 20 hang ngay — xem TECH_REFERENCE.md §12.2)
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models import Param

from lzd_utils.callbacks import DEFAULT_ARGS

DOC = __doc__

#: Thu muc artifact cua Track A. Nam tren volume dung chung, KHONG phai /tmp
#: cua container — task sau phai doc lai duoc.
DEFAULT_OUTPUT_DIR = "/opt/lakehouse/track_a"


@dag(
    dag_id="60_reconstruction_e2e",
    description="Track A backfill tren train split that + contract dry-run",
    schedule=None,                       # chi trigger tay
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    default_args={**DEFAULT_ARGS, "retries": 0},
    tags=["reconstruction", "track-a", "track-b", "lzd"],
    doc_md=DOC,
    params={
        "mode": Param(
            "contract", type="string", enum=["contract", "backfill"],
            description="contract = dry-run 1 target, khong cham ha tang. "
                        "backfill = train split that + GHI Postgres/DuckDB/MinIO.",
        ),
        "row_limit": Param(
            5000, type="integer",
            description="So dong train xu ly o mode backfill. 0 = toan bo "
                        "926,669 dong (~17.9M event).",
        ),
        "verify_limit": Param(
            300, type="integer",
            description="So dong chay qua SQL dbt that + Gate A de xac minh.",
        ),
        "dt": Param("2026-08-05", type="string",
                    description="Partition dt cua raw/events_v2 tren lake"),
        "land": Param(
            True, type="boolean",
            description="False = chi sinh artifact, KHONG ghi ha tang. "
                        "Dung de xem truoc khoi luong.",
        ),
    },
)
def reconstruction_e2e():

    @task
    def contract_dry_run(**context) -> dict:
        """Bai kiem HOP DONG — 1 target synthetic, khong cham ha tang."""
        import json

        from lzd_pipeline.reconstruction.e2e import run_demo

        if context["params"]["mode"] != "contract":
            return {"skipped": "mode != contract"}

        result = run_demo()
        summary = result.summary()
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        if not result.all_passed:
            raise RuntimeError(f"reconstruction gates failed: {summary['gates']}")
        return summary

    @task(pool="duckdb_writer", execution_timeout=pendulum.duration(hours=6))
    def backfill_train_split(**context) -> dict:
        """Track A tren train split THAT -> artifact -> ha tang.

        Chay trong pool `duckdb_writer` vi buoc land mo DuckDB o che do ghi.
        """
        import json
        from pathlib import Path

        from lzd_pipeline.common.clients import duckdb_writer
        from lzd_pipeline.common.logging_setup import get_logger
        from lzd_pipeline.reconstruction.e2e import load_runtime_config
        from lzd_pipeline.reconstruction.sink import land_track_a
        from lzd_pipeline.reconstruction.track_a_batch import (
            DEFAULT_INPUT,
            materialize_track_a,
        )

        params = context["params"]
        if params["mode"] != "backfill":
            return {"skipped": "mode != backfill"}

        log = get_logger(__name__)
        limit = int(params["row_limit"]) or None
        output_dir = Path(DEFAULT_OUTPUT_DIR)

        manifest = materialize_track_a(
            input_path=DEFAULT_INPUT,
            output_dir=output_dir,
            limit=limit,
            config=load_runtime_config(),
            verify_limit=int(params["verify_limit"]),
        )
        log.info("Track A backfill xong", extra={
            "event": "track_a_backfill",
            "processed_rows": manifest["processed_rows"],
            "solved_rows": manifest["solved_rows"],
            "quarantined_rows": manifest["quarantined_rows"],
            "raw_events": manifest["raw_events"],
        })

        gate = manifest["gate_sample"]
        if not gate["all_passed"]:
            raise RuntimeError(
                f"Gate A do tren mau {gate['checked_rows']} dong: "
                f"{gate['failures'][:5]}. 🚫 KHONG land du lieu khong qua Gate A."
            )

        if not params["land"]:
            return {"manifest": manifest, "landed": "SKIPPED (land=False)"}

        with duckdb_writer() as con:
            report = land_track_a(output_dir, params["dt"], con=con)
        print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
        return {"manifest": manifest, "landed": report}

    @task(pool="duckdb_writer")
    def assert_sources_ready(**context) -> dict:
        """Chan viec doc mart reconstruction rong nhu the no co nghia.

        Chay SAU backfill. Neu `land=False` thi buoc nay CO LE do — dung y do.
        """
        from lzd_pipeline.common.clients import duckdb_writer
        from lzd_pipeline.reconstruction.warehouse import (
            assert_reconstruction_sources_ready,
        )

        params = context["params"]
        if params["mode"] != "backfill" or not params["land"]:
            return {"skipped": "khong land thi khong co gi de kiem"}

        with duckdb_writer() as con:
            return assert_reconstruction_sources_ready(con)

    contract_dry_run() >> backfill_train_split() >> assert_sources_ready()


reconstruction_e2e()
