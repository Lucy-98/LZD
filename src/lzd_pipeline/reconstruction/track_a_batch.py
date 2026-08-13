"""Materialize Track A reconstructed raw events from the real train CSV.

This is the host-side path used when Docker/MinIO is not available. It reads the
real `data/full_trainset.csv`, decodes the 36 selected reconstruction features,
emits CFS raw events plus the sidecar tables needed by the dbt reconstruction
models, and verifies a configurable sample through the same dbt SQL runner.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import duckdb

from lzd_pipeline.reconstruction import engine, runner
from lzd_pipeline.reconstruction.canonical import float_repr
from lzd_pipeline.reconstruction.candidate import Candidate, Slot
from lzd_pipeline.reconstruction.e2e import RuntimeConfig, load_runtime_config
from lzd_pipeline.reconstruction.feature_set import SelectedFeatureSet, load_feature_set
from lzd_pipeline.reconstruction.semantics import DecodedTarget, build_branch
from lzd_pipeline.reconstruction.target import ReconstructionTarget, build_target

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_INPUT = ROOT / "data" / "full_trainset.csv"
DEFAULT_OUTPUT_DIR = ROOT / ".tmp" / "reconstruction_track_a"


@dataclass(frozen=True)
class ValueMap:
    attr_name: str
    columns: tuple[str, ...]
    to_level: Mapping[tuple[str, ...], int]
    to_values: Mapping[int, tuple[float, ...]]

    def decode(self, row: Mapping[str, str]) -> int:
        key = tuple(float_repr(float(row[c])) for c in self.columns)
        return self.to_level[key]

    def encoding_rows(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for level, values in sorted(self.to_values.items()):
            for column, value in zip(self.columns, values):
                out.append({
                    "attr_name": self.attr_name,
                    "level_id": level,
                    "column_name": column,
                    "value": float_repr(value),
                })
        return out


@dataclass(frozen=True)
class RowResult:
    target: ReconstructionTarget
    decoded: DecodedTarget
    attributes: Mapping[str, int]
    passthrough: Mapping[str, float]
    outcome: engine.SolveOutcome
    generation_run_id: str


@dataclass(frozen=True)
class GateSummary:
    checked_rows: int
    gate_a_passed: int
    gate_a_t3_passed: int
    failures: tuple[dict[str, Any], ...]

    @property
    def all_passed(self) -> bool:
        return (
            self.checked_rows > 0
            and self.gate_a_passed == self.checked_rows
            and self.gate_a_t3_passed == self.checked_rows
            and not self.failures
        )


def _iter_rows(path: Path, limit: int | None = None) -> Iterable[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for index, row in enumerate(reader):
            if limit is not None and index >= limit:
                break
            yield row


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]], fields: Sequence[str]) -> int:
    count = 0
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(fields))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def fit_value_maps(
    input_path: Path,
    fs: SelectedFeatureSet,
    *,
    limit: int | None,
) -> dict[str, ValueMap]:
    value_attrs = {
        name: attr for name, attr in fs.source_attributes.items()
        if attr.encoding != "one_hot"
    }
    seen: dict[str, dict[tuple[str, ...], tuple[float, ...]]] = {
        name: {} for name in value_attrs
    }
    for row in _iter_rows(input_path, limit):
        for name, attr in value_attrs.items():
            values = tuple(float(row[c]) for c in attr.outputs)
            key = tuple(float_repr(v) for v in values)
            seen[name].setdefault(key, values)

    maps: dict[str, ValueMap] = {}
    for name, attr in value_attrs.items():
        ordered = sorted(seen[name])
        maps[name] = ValueMap(
            attr_name=name,
            columns=tuple(attr.outputs),
            to_level={key: idx for idx, key in enumerate(ordered)},
            to_values={idx: seen[name][key] for idx, key in enumerate(ordered)},
        )
    return maps


def decode_target(values: Mapping[str, float]) -> DecodedTarget:
    return DecodedTarget(
        n5=round(math.exp(values["f5"])),
        n11=round(math.exp(values["f11"])),
        n18=round(pow(10.0, values["f18"])),
        n30=round(pow(10.0, values["f30"])),
        d1=round(values["f1"]),
        d2=round(values["f2"]),
        window_days=30,
    )


def decode_attributes(
    row: Mapping[str, str],
    fs: SelectedFeatureSet,
    value_maps: Mapping[str, ValueMap],
) -> dict[str, int]:
    attrs: dict[str, int] = {}
    for name, spec in fs.source_attributes.items():
        if spec.encoding == "one_hot":
            hot = [
                idx for idx, column in enumerate(spec.outputs)
                if float(row[column]) == 1.0
            ]
            if len(hot) != 1:
                raise ValueError(f"{name}: one-hot invalid, hot_count={len(hot)}")
            attrs[name] = hot[0]
        else:
            attrs[name] = value_maps[name].decode(row)
    return attrs


def construct_h1_candidate(decoded: DecodedTarget) -> Candidate:
    forced = sorted(decoded.forced_days)
    if len(forced) > decoded.n30:
        raise ValueError(
            f"H1 infeasible: n30={decoded.n30} < forced_days={len(forced)}"
        )

    active_days = list(forced)
    for day in range(decoded.window_days):
        if len(active_days) >= decoded.n30:
            break
        if day not in decoded.forced_days:
            active_days.append(day)
    active_days = sorted(active_days)
    if not active_days:
        raise ValueError("H1 infeasible: no active day")

    slots: list[Slot] = [
        Slot("recency", day, 1) for day in sorted({decoded.d1, decoded.d2})
    ]

    cursor = 0
    for reason, total in (
        ("f5", decoded.n5),
        ("f11", decoded.n11),
        ("f18", decoded.n18),
    ):
        per_day = {day: 0 for day in active_days}
        for idx in range(total):
            per_day[active_days[cursor % len(active_days)]] += 1
            cursor += 1
        slots.extend(Slot(reason, day, count) for day, count in per_day.items() if count)

    candidate = Candidate.of(slots)
    covered = candidate.day_offsets & set(range(decoded.window_days))
    missing = set(active_days) - covered
    if missing:
        candidate = Candidate.of((
            *candidate.slots,
            *(Slot("FREE", day, 1) for day in sorted(missing)),
        ))

    branch = build_branch("H1")
    if not branch.is_feasible(candidate, decoded):
        raise ValueError("constructive H1 candidate failed feasibility")
    return candidate


def reconstruct_row(
    row: Mapping[str, str],
    *,
    fs: SelectedFeatureSet,
    value_maps: Mapping[str, ValueMap],
    config: RuntimeConfig,
) -> RowResult:
    target_id = row["data_id"]
    values = {column: float(row[column]) for column in fs.columns}
    target = build_target(
        target_id=target_id,
        values=values,
        reference_ts=config.reference_ts,
        feature_version="full_trainset_csv_v1",
        fs=fs,
    )
    decoded = decode_target(values)
    attrs = decode_attributes(row, fs, value_maps)
    passthrough = {column: values[column] for column in fs.tiers["T3"]}

    if config.semantic_branch != "H1":
        raise ValueError("real Track A batch currently supports semantic_branch=H1")
    candidate = construct_h1_candidate(decoded)
    events = engine.materialize(
        candidate,
        target_id=target.target_id,
        seed=config.generation_seed,
        reference_ts=target.reference_ts,
    )
    outcome = engine.SolveOutcome(
        status="SOLVED",
        events=tuple(events),
        candidate=candidate,
        pool_size=1,
        objective_summary=engine.ObjectiveSummary(
            unexplained_events=candidate.unexplained_events,
            active_days=candidate.active_days_in_window(decoded.window_days),
            sessions=candidate.sessions,
            total_events=candidate.total_events,
            objective_value=build_branch("H1").objective(candidate),
        ),
    )
    generation_run_id = f"{config.version}:{target.target_id}:H1:track_a_real"
    return RowResult(
        target=target,
        decoded=decoded,
        attributes=attrs,
        passthrough=passthrough,
        outcome=outcome,
        generation_run_id=generation_run_id,
    )


def _raw_event_rows(result: RowResult) -> Iterable[dict[str, Any]]:
    for event in result.outcome.events:
        ts = event.event_ts.timestamp()
        yield {
            "event_id": event.event_id,
            "customer_id": result.target.target_id,
            "target_id": result.target.target_id,
            "event_type": event.event_type,
            "event_ts": format(ts, ".6f"),
            "observation_ts": format(ts, ".6f"),
            "source_type": "RECONSTRUCTED",
            "generation_run_id": result.generation_run_id,
        }


def _audit_event_rows(result: RowResult) -> Iterable[dict[str, Any]]:
    for event in result.outcome.events:
        yield {
            "event_id": event.event_id,
            "customer_id": result.target.target_id,
            "target_id": result.target.target_id,
            "event_type": event.event_type,
            "event_ts": event.event_ts.isoformat(),
            "source_type": "RECONSTRUCTED",
            "generation_run_id": result.generation_run_id,
            "gen_reason": event.gen_reason,
            "day_offset": event.day_offset,
            "sub_index": event.sub_index,
            "occurrence": event.occurrence,
        }


def verify_sample(
    samples: Sequence[RowResult],
    *,
    config: RuntimeConfig,
    value_maps: Mapping[str, ValueMap],
    fs: SelectedFeatureSet,
) -> GateSummary:
    if not samples:
        return GateSummary(0, 0, 0, ())

    encoding_rows = [
        (row["attr_name"], int(row["level_id"]), row["column_name"], float(row["value"]))
        for value_map in value_maps.values()
        for row in value_map.encoding_rows()
    ]
    onehot_rows = [
        (attr.name, idx, column)
        for attr in fs.source_attributes.values()
        if attr.encoding == "one_hot"
        for idx, column in enumerate(attr.outputs)
    ]

    with duckdb.connect(":memory:") as con:
        runner.seed(
            con,
            [
                runner.RunInput(
                    target_id=item.target.target_id,
                    decoded=item.decoded,
                    reference_ts=item.target.reference_ts,
                    attributes=item.attributes,
                    passthrough=item.passthrough,
                    generation_run_id=item.generation_run_id,
                )
                for item in samples
            ],
            {item.target.target_id: item.outcome for item in samples},
            encoding_rows=encoding_rows,
            onehot_rows=onehot_rows,
            encoding_version=config.encoding_version,
        )
        runner.build_marts(
            con,
            semantic_branch=config.semantic_branch,
            history_days=30,
            encoding_version=config.encoding_version,
        )
        reports = [
            (item, runner.gate_a(
                item.target.values,
                runner.read_reconstructed(con, item.target.target_id),
                fs,
            ))
            for item in samples
        ]

    failures: list[dict[str, Any]] = []
    for item, report in reports:
        for failure in report.failures():
            failures.append({
                "target_id": item.target.target_id,
                "column": failure.column,
                "regime": failure.regime,
                "expected": failure.expected,
                "actual": failure.actual,
            })
    return GateSummary(
        checked_rows=len(reports),
        gate_a_passed=sum(1 for _, report in reports if report.gate_a_pass),
        gate_a_t3_passed=sum(1 for _, report in reports if report.gate_a_t3_pass),
        failures=tuple(failures),
    )


def materialize_track_a(
    *,
    input_path: Path,
    output_dir: Path,
    limit: int | None,
    config: RuntimeConfig,
    verify_limit: int,
    write_parquet: bool = False,
) -> dict[str, Any]:
    fs = load_feature_set()
    output_dir.mkdir(parents=True, exist_ok=True)

    value_maps = fit_value_maps(input_path, fs, limit=limit)

    raw_path = output_dir / "raw_events_v2.csv"
    audit_path = output_dir / "track_a_events_audit.csv"
    boundary_path = output_dir / "biz_reconstruction_boundary.csv"
    attrs_path = output_dir / "biz_customer_attribute.csv"
    pass_path = output_dir / "biz_passthrough_source.csv"
    target_path = output_dir / "targets_selected_features.csv"
    quarantine_path = output_dir / "quarantine.csv"

    samples: list[RowResult] = []
    processed = 0
    solved = 0
    quarantined = 0
    raw_events = 0

    with raw_path.open("w", newline="", encoding="utf-8") as raw_fh, \
         audit_path.open("w", newline="", encoding="utf-8") as audit_fh, \
         boundary_path.open("w", newline="", encoding="utf-8") as boundary_fh, \
         attrs_path.open("w", newline="", encoding="utf-8") as attrs_fh, \
         pass_path.open("w", newline="", encoding="utf-8") as pass_fh, \
         target_path.open("w", newline="", encoding="utf-8") as target_fh, \
         quarantine_path.open("w", newline="", encoding="utf-8") as quarantine_fh:

        raw_writer = csv.DictWriter(raw_fh, fieldnames=[
            "event_id", "customer_id", "target_id", "event_type", "event_ts",
            "observation_ts", "source_type", "generation_run_id",
        ])
        audit_writer = csv.DictWriter(audit_fh, fieldnames=[
            "event_id", "customer_id", "target_id", "event_type", "event_ts",
            "source_type", "generation_run_id", "gen_reason", "day_offset",
            "sub_index", "occurrence",
        ])
        boundary_writer = csv.DictWriter(boundary_fh, fieldnames=[
            "target_id", "customer_id_hint", "reference_ts",
        ])
        attrs_writer = csv.DictWriter(attrs_fh, fieldnames=[
            "target_id", "attr_name", "level_id",
        ])
        pass_writer = csv.DictWriter(pass_fh, fieldnames=[
            "target_id", *fs.tiers["T3"],
        ])
        target_writer = csv.DictWriter(target_fh, fieldnames=[
            "target_id", "target_hash", *fs.columns,
        ])
        quarantine_writer = csv.DictWriter(quarantine_fh, fieldnames=[
            "target_id", "reason",
        ])
        for writer in (
            raw_writer, audit_writer, boundary_writer, attrs_writer,
            pass_writer, target_writer, quarantine_writer,
        ):
            writer.writeheader()

        for row in _iter_rows(input_path, limit):
            processed += 1
            target_id = row.get("data_id", f"row_{processed - 1}")
            try:
                result = reconstruct_row(
                    row,
                    fs=fs,
                    value_maps=value_maps,
                    config=config,
                )
            except Exception as exc:  # keep bad real rows inspectable
                quarantined += 1
                quarantine_writer.writerow({"target_id": target_id, "reason": str(exc)})
                continue

            solved += 1
            if len(samples) < verify_limit:
                samples.append(result)
            boundary_writer.writerow({
                "target_id": result.target.target_id,
                "customer_id_hint": result.target.target_id,
                "reference_ts": result.target.reference_ts.isoformat(),
            })
            for name, level in sorted(result.attributes.items()):
                attrs_writer.writerow({
                    "target_id": result.target.target_id,
                    "attr_name": name,
                    "level_id": level,
                })
            pass_writer.writerow({
                "target_id": result.target.target_id,
                **{column: float_repr(value) for column, value in result.passthrough.items()},
            })
            target_writer.writerow({
                "target_id": result.target.target_id,
                "target_hash": result.target.target_hash,
                **{column: float_repr(result.target.values[column]) for column in fs.columns},
            })
            for event_row in _raw_event_rows(result):
                raw_writer.writerow(event_row)
                raw_events += 1
            for event_row in _audit_event_rows(result):
                audit_writer.writerow(event_row)

    encoding_map_path = output_dir / "biz_encoding_map.csv"
    encoding_rows = (
        {
            "encoding_version": config.encoding_version,
            **row,
        }
        for value_map in value_maps.values()
        for row in value_map.encoding_rows()
    )
    encoding_rows_count = _write_csv(encoding_map_path, encoding_rows, [
        "encoding_version", "attr_name", "level_id", "column_name", "value",
    ])

    onehot_path = output_dir / "biz_onehot_layout.csv"
    onehot_rows = (
        {
            "encoding_version": config.encoding_version,
            "attr_name": attr.name,
            "level_index": idx,
            "column_name": column,
        }
        for attr in fs.source_attributes.values()
        if attr.encoding == "one_hot"
        for idx, column in enumerate(attr.outputs)
    )
    onehot_rows_count = _write_csv(onehot_path, onehot_rows, [
        "encoding_version", "attr_name", "level_index", "column_name",
    ])

    gate_summary = verify_sample(
        samples,
        config=config,
        value_maps=value_maps,
        fs=fs,
    )

    parquet_path = None
    if write_parquet:
        parquet_path = output_dir / "raw_events_v2.parquet"
        with duckdb.connect(":memory:") as con:
            con.execute(
                "COPY (SELECT * FROM read_csv_auto(?)) TO ? (FORMAT PARQUET)",
                [str(raw_path), str(parquet_path)],
            )

    manifest = {
        "schema_version": "track_a_real_backfill_v1",
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "limit": limit,
        "runtime_config_version": config.version,
        "semantic_branch": config.semantic_branch,
        "reference_ts": config.reference_ts.isoformat(),
        "selected_feature_set_id": fs.id,
        "selected_feature_count": len(fs.columns),
        "processed_rows": processed,
        "solved_rows": solved,
        "quarantined_rows": quarantined,
        "raw_events": raw_events,
        "encoding_map_rows": encoding_rows_count,
        "onehot_layout_rows": onehot_rows_count,
        "gate_sample": {
            "checked_rows": gate_summary.checked_rows,
            "gate_a_passed": gate_summary.gate_a_passed,
            "gate_a_t3_passed": gate_summary.gate_a_t3_passed,
            "all_passed": gate_summary.all_passed,
            "failures": gate_summary.failures[:20],
        },
        "files": {
            "raw_events_v2": str(raw_path),
            "track_a_events_audit": str(audit_path),
            "biz_reconstruction_boundary": str(boundary_path),
            "biz_customer_attribute": str(attrs_path),
            "biz_passthrough_source": str(pass_path),
            "biz_encoding_map": str(encoding_map_path),
            "biz_onehot_layout": str(onehot_path),
            "targets_selected_features": str(target_path),
            "quarantine": str(quarantine_path),
            "raw_events_v2_parquet": str(parquet_path) if parquet_path else None,
        },
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(_jsonable(manifest), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--all", action="store_true", help="process the whole train CSV")
    parser.add_argument("--verify-limit", type=int, default=50)
    parser.add_argument("--parquet", action="store_true")
    parser.add_argument("--config", type=Path)
    args = parser.parse_args()

    if args.limit is None and not args.all:
        raise SystemExit("Pass --limit N for pilot, or --all for the full train CSV.")
    if args.limit is not None and args.all:
        raise SystemExit("Use either --limit or --all, not both.")

    config = load_runtime_config(args.config) if args.config else load_runtime_config()
    manifest = materialize_track_a(
        input_path=args.input,
        output_dir=args.output_dir,
        limit=None if args.all else args.limit,
        config=config,
        verify_limit=args.verify_limit,
        write_parquet=args.parquet,
    )
    print(json.dumps(_jsonable(manifest), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
