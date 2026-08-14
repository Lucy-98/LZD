"""Runnable Track A -> CustomerState(T0) -> Track B dry-run.

This module is the orchestration boundary. Track B remains isolated in
``live.py`` and receives only ``CustomerState``; it never receives the target.
The forward pass executes the real dbt model SQL through ``runner.py``.
"""
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

import duckdb
import yaml

from lzd_pipeline.reconstruction import engine, persistence, runner
from lzd_pipeline.reconstruction.feature_set import SelectedFeatureSet, load_feature_set
from lzd_pipeline.reconstruction.handoff import GateReport, to_customer_state
from lzd_pipeline.reconstruction.live import FutureEvent, RuleBasedBehaviour, live_generator
from lzd_pipeline.reconstruction.semantics import DecodedTarget, build_branch
from lzd_pipeline.reconstruction.state import CustomerState, Provenance, SemanticStatus
from lzd_pipeline.reconstruction.target import ReconstructionTarget, SolverConfig, build_target

DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parents[3] / "config" / "reconstruction" / "runtime.yml"
)


@dataclass(frozen=True)
class RuntimeConfig:
    version: str
    semantic_status: SemanticStatus
    semantic_branch: str
    reference_ts: datetime
    future_days: int
    generation_seed: int
    constraint_model_version: str
    encoding_version: str
    objective_version: str
    selection_policy_version: str
    solver_version: str
    behaviour_policy_version: str

    def __post_init__(self) -> None:
        if self.reference_ts.tzinfo is None:
            raise ValueError("reference_ts must include a timezone")
        if self.future_days < 1:
            raise ValueError("future_days must be >= 1")
        self.solver_config()

    def solver_config(self) -> SolverConfig:
        return SolverConfig(
            semantic_branch=self.semantic_branch,  # type: ignore[arg-type]
            semantic_status=self.semantic_status,
            constraint_model_version=self.constraint_model_version,
            encoding_version=self.encoding_version,
            objective_version=self.objective_version,
            selection_policy_version=self.selection_policy_version,
            solver_version=self.solver_version,
            generation_seed=self.generation_seed,
        )


def load_runtime_config(path: Path | str = DEFAULT_CONFIG_PATH) -> RuntimeConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return RuntimeConfig(
        version=str(raw["version"]),
        semantic_status=raw["semantic_status"],
        semantic_branch=str(raw["semantic_branch"]),
        reference_ts=datetime.fromisoformat(raw["reference_ts"]),
        future_days=int(raw["future_days"]),
        generation_seed=int(raw["generation_seed"]),
        constraint_model_version=str(raw["constraint_model_version"]),
        encoding_version=str(raw["encoding_version"]),
        objective_version=str(raw["objective_version"]),
        selection_policy_version=str(raw["selection_policy_version"]),
        solver_version=str(raw["solver_version"]),
        behaviour_policy_version=str(raw["behaviour_policy_version"]),
    )


@dataclass(frozen=True)
class EndToEndResult:
    target: ReconstructionTarget
    outcome: engine.SolveOutcome
    reconstructed: Mapping[str, float]
    gate_a: runner.GateAReport
    handoff_gates: GateReport
    gate_d: bool
    gate_e: bool
    gate_f: bool
    state: CustomerState
    future_events: tuple[FutureEvent, ...]

    @property
    def all_passed(self) -> bool:
        return (
            self.handoff_gates.all_passed
            and self.gate_d
            and self.gate_e
            and self.gate_f
        )

    def summary(self) -> dict[str, Any]:
        return {
            "target_id": self.target.target_id,
            "semantic_status": self.state.semantic_status,
            "semantic_branch": self.state.semantic_branch,
            "track_a_status": self.outcome.status,
            "track_a_events": len(self.outcome.events),
            "track_b_events": len(self.future_events),
            "gates": {
                "A": self.gate_a.gate_a_pass,
                "A-T3": self.gate_a.gate_a_t3_pass,
                "B": self.handoff_gates.gate_b,
                "C": self.handoff_gates.gate_c,
                "D": self.gate_d,
                "E": self.gate_e,
                "F": self.gate_f,
            },
            "all_passed": self.all_passed,
            "track_b_generation_run_id": (
                self.future_events[0].generation_run_id if self.future_events else None
            ),
        }


def run_end_to_end(
    *,
    target: ReconstructionTarget,
    decoded: DecodedTarget,
    customer_id: str,
    attributes: Mapping[str, int],
    passthrough: Mapping[str, float],
    encoding_rows: Sequence[tuple[str, int, str, float]],
    onehot_rows: Sequence[tuple[str, int, str]],
    config: RuntimeConfig,
    behaviour: RuleBasedBehaviour | None = None,
) -> EndToEndResult:
    """Execute both tracks without writing Kafka, Redis, MinIO, or Postgres."""
    target.verify()
    solver_config = config.solver_config()
    branch = build_branch(solver_config.semantic_branch)
    generation_run_id = f"{config.version}:{target.target_id}:{config.semantic_branch}"

    outcome = engine.solve(
        decoded,
        branch,
        target_id=target.target_id,
        seed=solver_config.generation_seed,
        reference_ts=target.reference_ts,
    )

    with duckdb.connect(":memory:") as con:
        runner.seed(
            con,
            [runner.RunInput(
                target_id=target.target_id,
                decoded=decoded,
                reference_ts=target.reference_ts,
                attributes=attributes,
                passthrough=passthrough,
                generation_run_id=generation_run_id,
            )],
            {target.target_id: outcome},
            encoding_rows=encoding_rows,
            onehot_rows=onehot_rows,
            encoding_version=config.encoding_version,
        )
        runner.build_marts(
            con,
            semantic_branch=config.semantic_branch,
            history_days=decoded.window_days,
            encoding_version=config.encoding_version,
        )
        reconstructed = runner.read_reconstructed(con, target.target_id)

    gate_a_report = runner.gate_a(target.values, reconstructed)
    gate_b = bool(outcome.events) and all(
        event.event_ts < target.reference_ts for event in outcome.events
    )
    gate_c = (
        outcome.candidate is not None
        and branch.is_feasible(outcome.candidate, decoded)
    )
    handoff_gates = GateReport(
        gate_a=gate_a_report.gate_a_pass,
        gate_a_t3=gate_a_report.gate_a_t3_pass,
        gate_b=gate_b,
        gate_c=gate_c,
    )

    track_a_provenance = Provenance(
        source_type="RECONSTRUCTED",
        generation_run_id=generation_run_id,
        root_generation_id=generation_run_id,
        parent_target_id=target.target_id,
        parent_entity_id=customer_id,
        parent_feature_version=target.feature_version,
        created_at=target.reference_ts,
    )
    state = to_customer_state(
        outcome,
        decoded,
        handoff_gates,
        customer_id=customer_id,
        target_id=target.target_id,
        reference_ts=target.reference_ts,
        attributes=dict(attributes),
        semantic_branch=solver_config.semantic_branch,
        semantic_status=solver_config.semantic_status,
        provenance=track_a_provenance,
    )

    behaviour = behaviour or RuleBasedBehaviour(
        policy_version=config.behaviour_policy_version
    )
    if behaviour.policy_version != config.behaviour_policy_version:
        raise ValueError("behaviour policy does not match runtime config")
    future = tuple(live_generator(
        state,
        behaviour,
        state.as_of_ts,
        state.as_of_ts + timedelta(days=config.future_days),
        rng_seed=config.generation_seed,
    ))

    provenance_rows = [persistence.provenance_to_row(track_a_provenance)]
    if future:
        provenance_rows.append(persistence.provenance_to_row(future[0].provenance))
    persistence.check_all(provenance_rows)
    gate_d = all(event.provenance.source_type == "SYNTHETIC" for event in future)
    gate_e = not ({"label", "is_treat"} & set(target.values))
    gate_f = (
        0 < len(outcome.events) < 100_000
        and 0 < len(future) < 100_000
        and all(event.event_ts >= state.as_of_ts for event in future)
    )

    return EndToEndResult(
        target=target,
        outcome=outcome,
        reconstructed=reconstructed,
        gate_a=gate_a_report,
        handoff_gates=handoff_gates,
        gate_d=gate_d,
        gate_e=gate_e,
        gate_f=gate_f,
        state=state,
        future_events=future,
    )


def _demo_contract(
    fs: SelectedFeatureSet,
) -> tuple[
    DecodedTarget,
    dict[str, int],
    dict[str, float],
    list[tuple[str, int, str, float]],
    list[tuple[str, int, str]],
    dict[str, float],
]:
    decoded = DecodedTarget(
        n5=2, n11=1, n18=1, n19=1, n30=2, d1=1, d2=1, window_days=4
    )
    attributes = {
        "synthetic_category_515": 0,
        "synthetic_attr_64": 0,
        "synthetic_attr_241": 0,
        "synthetic_segment_g1": 0,
        "synthetic_segment_g2": 1,
        "synthetic_segment_g3": 0,
        "synthetic_segment_g4": 1,
        "synthetic_segment_g6": 0,
    }
    encoded_values = {
        "synthetic_category_515": (0.79, 0.80, 0.81, 0.82),
        "synthetic_attr_64": (0.37,),
        "synthetic_attr_241": (0.38,),
    }

    values: dict[str, float] = {
        "f1": float(decoded.d1),
        "f2": float(decoded.d2),
        "f5": math.log(decoded.n5),
        "f11": math.log(decoded.n11),
        "f18": round(math.log10(decoded.n18), 6),
        "f30": round(math.log10(decoded.n30), 6),
    }
    if decoded.n19 is not None:
        values["f19"] = round(math.log10(decoded.n19), 6)
    encoding_rows: list[tuple[str, int, str, float]] = []
    onehot_rows: list[tuple[str, int, str]] = []

    for attr_name, attr in fs.source_attributes.items():
        level = attributes[attr_name]
        if attr.encoding == "one_hot":
            for index, column in enumerate(attr.outputs):
                onehot_rows.append((attr_name, index, column))
                if column in attr.selected:
                    values[column] = 1.0 if index == level else 0.0
        else:
            for column, value in zip(attr.outputs, encoded_values[attr_name]):
                encoding_rows.append((attr_name, level, column, value))
                if column in attr.selected:
                    values[column] = value

    passthrough = {
        column: float(index + 1) / 100.0
        for index, column in enumerate(fs.tiers["T3"])
    }
    values.update(passthrough)
    return decoded, attributes, passthrough, encoding_rows, onehot_rows, values


def run_demo(config: RuntimeConfig | None = None) -> EndToEndResult:
    config = config or load_runtime_config()
    fs = load_feature_set()
    decoded, attributes, passthrough, enc, onehot, values = _demo_contract(fs)
    target = build_target(
        target_id="demo-target-001",
        values=values,
        reference_ts=config.reference_ts,
        feature_version="v1",
        fs=fs,
    )
    return run_end_to_end(
        target=target,
        decoded=decoded,
        customer_id="demo-customer-001",
        attributes=attributes,
        passthrough=passthrough,
        encoding_rows=enc,
        onehot_rows=onehot,
        config=config,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--branch", choices=("H1", "H2"))
    parser.add_argument("--future-days", type=int)
    args = parser.parse_args()

    config = load_runtime_config(args.config)
    if args.branch:
        config = replace(config, semantic_branch=args.branch)
    if args.future_days is not None:
        config = replace(config, future_days=args.future_days)
    result = run_demo(config)
    print(json.dumps(result.summary(), indent=2, ensure_ascii=False))
    if not result.all_passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
