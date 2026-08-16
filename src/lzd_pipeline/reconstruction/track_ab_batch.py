"""Track A + Track B tren NGUOI DUNG THAT, chay ca hai chang cho tung user.

    python -m lzd_pipeline.reconstruction.track_ab_batch --limit 10

Khac hai duong da co san:

    e2e.py             ca hai chang, nhung MOT target tong hop (`demo-target-001`)
    track_a_batch.py   nhieu user that, nhung CHI Track A
    module nay         nhieu user that, CA HAI chang

Voi moi hang trong `data/full_trainset.csv`:

    feature T1  --(giai ma)-->  DecodedTarget
                --(solve_h1)->  Track A witness events      <- qua khu
                --(dbt SQL)-->  feature dung lai  -> Gate A
                --(handoff)-->  CustomerState(T0)  -> Gate B/C
                --(behaviour)>  Track B future events       <- tuong lai

★ VI SAO KHONG GOI THANG `run_demo()` CHO NHIEU USER
--------------------------------------------------------------------------
`run_end_to_end()` mac dinh goi `engine.solve()`, ma buoc dau cua no la liet
ke toan bo khong gian nghiem. Voi `window_days=30` cua du lieu that thi do la
median 10^9.5 candidate moi target. Nen o day dung nghiem tu solver
constructive (`track_a_batch.reconstruct_row`) va truyen vao qua tham so
`outcome=`. Cac gate va Track B khong doi mot dong nao.

★ DAY LA DRY-RUN
--------------------------------------------------------------------------
Khong ghi Kafka, Redis, MinIO hay Postgres. dbt SQL chay tren DuckDB
in-memory. Chay duoc khi ca stack Docker dang tat.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

from lzd_pipeline.common.logging_setup import get_logger
from lzd_pipeline.reconstruction.e2e import (
    RuntimeConfig,
    load_runtime_config,
    run_end_to_end,
)
from lzd_pipeline.reconstruction.feature_set import SelectedFeatureSet, load_feature_set
from lzd_pipeline.reconstruction.track_a_batch import (
    DEFAULT_INPUT,
    ValueMap,
    _iter_rows,
    fit_value_maps,
    lzd_user_id,
    reconstruct_row,
)

log = get_logger(__name__)


def _contract_rows(
    fs: SelectedFeatureSet, value_maps: dict[str, ValueMap]
) -> tuple[list[tuple[str, int, str, float]], list[tuple[str, int, str]]]:
    """Bang tra encoding + layout one-hot, dung chung cho moi user.

    Hai bang nay mo ta HOP DONG MA HOA, khong phai du lieu cua mot user —
    dung `fit_value_maps` tren cung tap hang thi moi user deu tra ve cung mot
    level cho cung mot gia tri. Dung lai giua cac user la co y.
    """
    encoding_rows: list[tuple[str, int, str, float]] = []
    onehot_rows: list[tuple[str, int, str]] = []

    for attr_name, attr in fs.source_attributes.items():
        if attr.encoding == "one_hot":
            for index, column in enumerate(attr.outputs):
                onehot_rows.append((attr_name, index, column))
        else:
            vm = value_maps[attr_name]
            for level, values in sorted(vm.to_values.items()):
                for column, value in zip(vm.columns, values):
                    encoding_rows.append((attr_name, level, column, float(value)))
    return encoding_rows, onehot_rows


#: Cot cua `track_b_events.csv`. Giu y het `snapshot._track_b_rows()` de mot
#: file duy nhat mo ta Track B, du sinh tu duong nao.
TRACK_B_COLUMNS = (
    "event_id", "customer_id", "event_type", "event_ts", "session_id",
    "source_type", "track", "generation_run_id", "root_generation_id",
    "behaviour_policy_version",
)


def _future_event_rows(result) -> list[dict[str, Any]]:
    """Track B events -> dict phang de ghi CSV.

    `source_type` LUON la SYNTHETIC — day la thu duy nhat phan biet no voi
    event that o moi tang phia sau.
    """
    return [
        {
            "event_id": e.event_id,
            "customer_id": e.customer_id,
            "event_type": e.event_type,
            "event_ts": e.event_ts.isoformat(),
            "session_id": e.session_id,
            "source_type": e.source_type,
            "track": e.track,
            "generation_run_id": e.generation_run_id,
            "root_generation_id": e.provenance.root_generation_id,
            "behaviour_policy_version": e.provenance.behaviour_policy_version or "",
        }
        for e in result.future_events
    ]


def write_track_b_csv(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    """Ghi `track_b_events.csv` — dau vao cua `sink.publish_track_b_to_lake()`."""
    import csv

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "track_b_events.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(TRACK_B_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)
    return path


def run_batch(
    *,
    limit: int = 10,
    input_path: Path = DEFAULT_INPUT,
    config: RuntimeConfig | None = None,
) -> dict[str, Any]:
    """Chay ca hai chang cho `limit` user dau tien. Tra ve bao cao tong hop."""
    config = config or load_runtime_config()
    fs = load_feature_set()

    # Fit tren dung tap hang se chay: level id phai on dinh trong pham vi lo
    # nay, khong can (va khong nen) doc het 926k hang chi de demo 10 user.
    value_maps = fit_value_maps(input_path, fs, limit=limit)
    encoding_rows, onehot_rows = _contract_rows(fs, value_maps)

    users: list[dict[str, Any]] = []
    future_rows: list[dict[str, Any]] = []
    for row in _iter_rows(input_path, limit):
        rr = reconstruct_row(row, fs=fs, value_maps=value_maps, config=config)
        result = run_end_to_end(
            target=rr.target,
            decoded=rr.decoded,
            customer_id=lzd_user_id(row["data_id"]),
            attributes=rr.attributes,
            passthrough=rr.passthrough,
            encoding_rows=encoding_rows,
            onehot_rows=onehot_rows,
            config=config,
            outcome=rr.outcome,          # nghiem constructive, xem docstring
        )
        summary = result.summary()
        users.append({
            "data_id": row["data_id"],
            "user_id": lzd_user_id(row["data_id"]),
            "window_days": rr.decoded.window_days,
            **summary,
        })
        future_rows.extend(_future_event_rows(result))
        log.info("da chay ca hai chang cho mot user",
                 extra={"event": "track_ab_user_done",
                        "user_id": lzd_user_id(row["data_id"]),
                        "all_passed": summary.get("all_passed")})

    passed = [u for u in users if u.get("all_passed")]
    return {
        "future_events": future_rows,
        "n_users": len(users),
        "n_all_gates_passed": len(passed),
        "future_days": config.future_days,
        "semantic_branch": config.semantic_branch,
        "track_a_events_total": sum(int(u.get("track_a_events", 0)) for u in users),
        "track_b_events_total": sum(int(u.get("track_b_events", 0)) for u in users),
        "users": users,
    }


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Track A + Track B cho N user that")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--future-days", type=int)
    parser.add_argument("--output", type=Path, help="ghi bao cao JSON ra file")
    parser.add_argument(
        "--land", type=Path, metavar="DIR",
        help="Ghi Track B ra CSV trong DIR roi day len lake (prefix RIENG "
             "raw/track_b_future/). Mac dinh: dry-run, khong cham ha tang.",
    )
    args = parser.parse_args()

    config = load_runtime_config()
    if args.future_days is not None:
        config = replace(config, future_days=args.future_days)

    report = run_batch(limit=args.limit, input_path=args.input, config=config)

    if args.land:
        # Mac dinh KHONG chay nhanh nay: sinh Track B la mot phep mo phong,
        # con day no len lake la mot hanh dong len ha tang.
        from lzd_pipeline.reconstruction.sink import publish_track_b_to_lake

        csv_path = write_track_b_csv(report["future_events"], args.land)
        landed = publish_track_b_to_lake(args.land, config.reference_ts.date().isoformat())
        print(f"\nTrack B -> {csv_path}")
        print(f"Track B -> {landed['target']} ({landed['rows']:,} event)")

    rows = report["users"]
    print(f"\n{'user_id':<26} {'A_events':>9} {'B_events':>9} {'gates':>6}")
    print("-" * 54)
    for u in rows:
        print(f"{u['user_id']:<26} {u.get('track_a_events', 0):>9} "
              f"{u.get('track_b_events', 0):>9} "
              f"{'PASS' if u.get('all_passed') else 'FAIL':>6}")
    print("-" * 54)
    print(f"{report['n_all_gates_passed']}/{report['n_users']} user qua het gate | "
          f"Track A {report['track_a_events_total']} event | "
          f"Track B {report['track_b_events_total']} event "
          f"({report['future_days']} ngay tuong lai)")

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        # Bo `future_events` ra khoi bao cao: do la DU LIEU, khong phai tom
        # tat. Muon giu thi dung `--land`, no ghi CSV dung dinh dang ma lake
        # doc duoc.
        summary_only = {k: v for k, v in report.items() if k != "future_events"}
        args.output.write_text(
            json.dumps(summary_only, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        print(f"\nbao cao: {args.output}")

    if report["n_all_gates_passed"] != report["n_users"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
