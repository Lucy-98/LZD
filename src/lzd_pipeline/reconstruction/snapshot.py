"""Create committed review artifacts for the reconstruction dry-run.

The reconstruction itself is deterministic and normally runs once for a given
runtime config. This module materializes a small, reviewable snapshot under
``docs/`` so someone who pulls the repo can inspect the reconstructed events,
future synthetic events, feature comparison, and charts without running Docker.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from lzd_pipeline.features.business_aliases import BusinessAliasSpec, load_business_aliases
from lzd_pipeline.reconstruction.e2e import EndToEndResult, RuntimeConfig
from lzd_pipeline.reconstruction.e2e import load_runtime_config, run_demo
from lzd_pipeline.reconstruction.feature_set import SelectedFeatureSet, load_feature_set
from lzd_pipeline.reconstruction.runner import ColumnResult

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = ROOT / "docs" / "reconstruction_snapshot"
SNAPSHOT_SCHEMA_VERSION = "reconstruction_snapshot_v1"


def _iso(ts: datetime | None) -> str:
    return "" if ts is None else ts.isoformat()


def _num(value: float | None) -> str:
    return "" if value is None else format(value, ".17g")


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(_jsonable(payload), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(fields))
        writer.writeheader()
        writer.writerows(rows)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _feature_rows(
    result: EndToEndResult,
    fs: SelectedFeatureSet,
    aliases: BusinessAliasSpec,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in (*result.gate_a.gate_a, *result.gate_a.gate_a_t3):
        tier = fs.tier_of(item.column)
        alias = aliases.for_feature(item.column)
        rows.append({
            "column": item.column,
            "business_alias": aliases.alias_of(item.column),
            "display_name": aliases.display_name_of(item.column),
            "tier": tier,
            "method": "event_reconstructed" if tier in {"T1", "T2"} else "pass_through",
            "regime": item.regime,
            "source_family": alias.source_family if alias else "",
            "business_role": alias.business_role if alias else "",
            "semantic_confidence": alias.confidence if alias else "",
            "reconstruction_note": alias.reconstruction_note if alias else "",
            "expected": _num(item.expected),
            "actual": _num(item.actual),
            "ok": str(item.ok).lower(),
        })
    return rows


def _track_a_rows(result: EndToEndResult, aliases: BusinessAliasSpec) -> list[dict[str, Any]]:
    run_id = result.state.provenance.generation_run_id
    return [
        {
            "event_id": event.event_id,
            "customer_id": result.state.customer_id,
            "target_id": result.target.target_id,
            "event_type": event.event_type,
            "event_family": "CFS_WITNESS",
            "event_alias": aliases.event_alias_of(event.event_type),
            "event_note": aliases.event_note_of(event.event_type),
            "event_ts": _iso(event.event_ts),
            "source_type": "RECONSTRUCTED",
            "generation_run_id": run_id,
            "gen_reason": event.gen_reason,
            "day_offset": event.day_offset,
            "sub_index": event.sub_index,
            "occurrence": event.occurrence,
        }
        for event in result.outcome.events
    ]


def _track_b_rows(result: EndToEndResult) -> list[dict[str, Any]]:
    return [
        {
            "event_id": event.event_id,
            "customer_id": event.customer_id,
            "event_type": event.event_type,
            "event_family": "BUSINESS_V2_SYNTHETIC",
            "event_alias": event.event_type,
            "event_note": "Synthetic future business event generated from CustomerState(T0).",
            "event_ts": _iso(event.event_ts),
            "session_id": event.session_id,
            "source_type": event.source_type,
            "track": event.track,
            "generation_run_id": event.generation_run_id,
            "parent_run_id": event.provenance.parent_run_id or "",
            "root_generation_id": event.provenance.root_generation_id,
            "behaviour_policy_version": event.provenance.behaviour_policy_version or "",
        }
        for event in result.future_events
    ]


def _event_timeline_svg(track_a: Sequence[Mapping[str, Any]],
                        track_b: Sequence[Mapping[str, Any]]) -> str:
    all_rows = [*track_a, *track_b]
    if not all_rows:
        return "<svg viewBox='0 0 900 160' role='img' aria-label='No events'></svg>"

    timestamps = [
        datetime.fromisoformat(str(row["event_ts"])) for row in all_rows
    ]
    lo = min(timestamps).timestamp()
    hi = max(timestamps).timestamp()
    if hi == lo:
        hi = lo + 1

    def x_for(ts_text: str) -> float:
        ts = datetime.fromisoformat(ts_text).timestamp()
        return 90 + ((ts - lo) / (hi - lo)) * 740

    circles: list[str] = []
    labels: list[str] = []
    for rows, y, color, label in (
        (track_a, 58, "#2b6cb0", "Track A"),
        (track_b, 118, "#2f855a", "Track B"),
    ):
        labels.append(
            f"<text x='16' y='{y + 5}' font-size='13' fill='#2d3748'>{label}</text>"
        )
        for row in rows:
            x = x_for(str(row["event_ts"]))
            kind = html.escape(str(row["event_type"]))
            circles.append(
                f"<circle cx='{x:.1f}' cy='{y}' r='6' fill='{color}'>"
                f"<title>{kind} | {html.escape(str(row['event_ts']))}</title>"
                "</circle>"
            )

    return (
        "<svg viewBox='0 0 900 170' role='img' "
        "aria-label='Track A and Track B event timeline'>"
        "<rect width='900' height='170' fill='#ffffff'/>"
        "<line x1='90' y1='58' x2='830' y2='58' stroke='#cbd5e0'/>"
        "<line x1='90' y1='118' x2='830' y2='118' stroke='#cbd5e0'/>"
        + "".join(labels)
        + "".join(circles)
        + f"<text x='90' y='154' font-size='12' fill='#4a5568'>{html.escape(min(timestamps).isoformat())}</text>"
        + f"<text x='700' y='154' font-size='12' fill='#4a5568'>{html.escape(max(timestamps).isoformat())}</text>"
        "</svg>"
    )


def _feature_bar_svg(rows: Sequence[Mapping[str, Any]]) -> str:
    totals = {
        tier: sum(1 for row in rows if row["tier"] == tier)
        for tier in ("T1", "T2", "T3")
    }
    passed = {
        tier: sum(1 for row in rows if row["tier"] == tier and row["ok"] == "true")
        for tier in ("T1", "T2", "T3")
    }
    max_total = max(totals.values()) if totals else 1
    bars: list[str] = []
    for idx, tier in enumerate(("T1", "T2", "T3")):
        x = 120 + idx * 210
        total_h = 120
        pass_h = (passed[tier] / max_total) * total_h
        y = 150 - pass_h
        bars.append(
            f"<rect x='{x}' y='{150 - total_h}' width='86' height='{total_h}' fill='#edf2f7'/>"
            f"<rect x='{x}' y='{y:.1f}' width='86' height='{pass_h:.1f}' fill='#2f855a'/>"
            f"<text x='{x + 43}' y='172' text-anchor='middle' font-size='13' fill='#2d3748'>{tier}</text>"
            f"<text x='{x + 43}' y='{y - 8:.1f}' text-anchor='middle' font-size='12' fill='#2d3748'>"
            f"{passed[tier]}/{totals[tier]}</text>"
        )
    return (
        "<svg viewBox='0 0 760 200' role='img' aria-label='Feature pass counts by tier'>"
        "<rect width='760' height='200' fill='#ffffff'/>"
        "<text x='24' y='28' font-size='14' fill='#2d3748'>Feature comparison pass count</text>"
        + "".join(bars)
        + "</svg>"
    )


def _summary_cards(summary: Mapping[str, Any], fs: SelectedFeatureSet) -> str:
    gates = summary["gates"]
    items = [
        ("Target", summary["target_id"]),
        ("Branch", summary["semantic_branch"]),
        ("Track A events", summary["track_a_events"]),
        ("Track B events", summary["track_b_events"]),
        ("Selected features", len(fs.columns)),
        ("All gates passed", summary["all_passed"]),
    ]
    gate_text = ", ".join(f"{name}={str(ok).lower()}" for name, ok in gates.items())
    cards = "".join(
        "<div class='card'>"
        f"<div class='label'>{html.escape(str(label))}</div>"
        f"<div class='value'>{html.escape(str(value))}</div>"
        "</div>"
        for label, value in items
    )
    return cards + f"<p class='muted'>Gates: {html.escape(gate_text)}</p>"


def _write_html_report(
    path: Path,
    *,
    result: EndToEndResult,
    config: RuntimeConfig,
    fs: SelectedFeatureSet,
    aliases: BusinessAliasSpec,
    feature_rows: Sequence[Mapping[str, Any]],
    track_a_rows: Sequence[Mapping[str, Any]],
    track_b_rows: Sequence[Mapping[str, Any]],
) -> None:
    summary = result.summary()
    feature_rows_html = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(row['column']))}</td>"
        f"<td>{html.escape(str(row['business_alias']))}</td>"
        f"<td>{html.escape(str(row['display_name']))}</td>"
        f"<td>{html.escape(str(row['tier']))}</td>"
        f"<td>{html.escape(str(row['method']))}</td>"
        f"<td>{html.escape(str(row['regime']))}</td>"
        f"<td>{html.escape(str(row['business_role']))}</td>"
        f"<td>{html.escape(str(row['semantic_confidence']))}</td>"
        f"<td>{html.escape(str(row['expected']))}</td>"
        f"<td>{html.escape(str(row['actual']))}</td>"
        f"<td>{html.escape(str(row['ok']))}</td>"
        "</tr>"
        for row in feature_rows
    )
    event_rows_html = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(row['source_type']))}</td>"
        f"<td>{html.escape(str(row['event_family']))}</td>"
        f"<td>{html.escape(str(row['event_type']))}</td>"
        f"<td>{html.escape(str(row['event_alias']))}</td>"
        f"<td>{html.escape(str(row['event_ts']))}</td>"
        f"<td>{html.escape(str(row['event_id']))}</td>"
        "</tr>"
        for row in [*track_a_rows, *track_b_rows]
    )
    body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>LZD Reconstruction Snapshot</title>
  <style>
    body {{ margin: 0; font: 14px/1.45 Arial, sans-serif; color: #1a202c; background: #f7fafc; }}
    main {{ max-width: 1100px; margin: 0 auto; padding: 32px 20px 56px; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    h2 {{ margin: 28px 0 12px; font-size: 18px; }}
    a {{ color: #2b6cb0; }}
    .muted {{ color: #4a5568; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; }}
    .card {{ background: #ffffff; border: 1px solid #e2e8f0; border-radius: 6px; padding: 12px; }}
    .label {{ color: #718096; font-size: 12px; text-transform: uppercase; }}
    .value {{ margin-top: 4px; font-size: 18px; font-weight: 700; }}
    .panel {{ background: #ffffff; border: 1px solid #e2e8f0; border-radius: 6px; padding: 16px; overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; background: #ffffff; }}
    th, td {{ border-bottom: 1px solid #e2e8f0; padding: 8px 10px; text-align: left; white-space: nowrap; }}
    th {{ background: #edf2f7; font-weight: 700; }}
    code {{ background: #edf2f7; padding: 2px 4px; border-radius: 4px; }}
  </style>
</head>
<body>
<main>
  <h1>LZD Reconstruction Snapshot</h1>
  <p class="muted">
    Committed review artifact generated from <code>lzd_pipeline.reconstruction.e2e.run_demo()</code>.
    It is not production lakehouse data and does not include model/API outputs.
  </p>
  <p class="muted">
    Business aliases come from <code>config/features/business_aliases.yml</code>.
    They are synthetic narrative names, not confirmed Lazada semantics.
  </p>
  <section class="grid">{_summary_cards(summary, fs)}</section>

  <h2>Timeline</h2>
  <div class="panel">{_event_timeline_svg(track_a_rows, track_b_rows)}</div>

  <h2>Feature Result</h2>
  <div class="panel">{_feature_bar_svg(feature_rows)}</div>
  <p class="muted">
    T1/T2 are reconstructed by generated events or source attributes. T3 is pass-through/frozen.
    The scope is exactly 36 selected features from <code>{html.escape(fs.id)}</code>, not all f0..f82.
    Alias set: <code>{html.escape(aliases.version)}</code>.
  </p>

  <h2>Files</h2>
  <ul>
    <li><a href="track_a_events.csv">track_a_events.csv</a></li>
    <li><a href="track_b_events.csv">track_b_events.csv</a></li>
    <li><a href="features_expected_actual.csv">features_expected_actual.csv</a></li>
    <li><a href="summary.json">summary.json</a></li>
    <li><a href="manifest.json">manifest.json</a></li>
  </ul>

  <h2>Feature Comparison</h2>
  <div class="panel">
    <table>
      <thead><tr><th>Feature</th><th>Alias</th><th>Display</th><th>Tier</th><th>Method</th><th>Regime</th><th>Business Role</th><th>Confidence</th><th>Expected</th><th>Actual</th><th>OK</th></tr></thead>
      <tbody>{feature_rows_html}</tbody>
    </table>
  </div>

  <h2>Events</h2>
  <div class="panel">
    <table>
      <thead><tr><th>Source</th><th>Family</th><th>Event Type</th><th>Alias</th><th>Event TS</th><th>Event ID</th></tr></thead>
      <tbody>{event_rows_html}</tbody>
    </table>
  </div>

  <h2>Runtime</h2>
  <div class="panel">
    <table>
      <tbody>
        <tr><th>Runtime config</th><td>{html.escape(config.version)}</td></tr>
        <tr><th>Reference TS</th><td>{html.escape(config.reference_ts.isoformat())}</td></tr>
        <tr><th>Future days</th><td>{config.future_days}</td></tr>
        <tr><th>Generation seed</th><td>{config.generation_seed}</td></tr>
        <tr><th>Solver</th><td>{html.escape(config.solver_version)}</td></tr>
        <tr><th>Behaviour policy</th><td>{html.escape(config.behaviour_policy_version)}</td></tr>
        <tr><th>Business alias set</th><td>{html.escape(aliases.version)}</td></tr>
      </tbody>
    </table>
  </div>
</main>
</body>
</html>
"""
    path.write_text(body, encoding="utf-8")


def _write_snapshot_readme(path: Path, manifest: Mapping[str, Any]) -> None:
    text = f"""# Reconstruction Snapshot

Small committed artifact for reviewing the deterministic reconstruction dry-run.

The charts below render directly in GitHub. Open `report.html` locally for the
full table view.

![Feature pass chart](feature_pass.svg)

![Event timeline](timeline.svg)

Files:

- `track_a_events.csv`: reconstructed Track A witness events.
- `track_b_events.csv`: future synthetic Track B events from `CustomerState(T0)`.
- `features_expected_actual.csv`: expected vs actual for the 36 selected features.
  It includes synthetic business aliases from `config/features/business_aliases.yml`.
- `feature_pass.svg`: pass-count chart by feature tier.
- `timeline.svg`: Track A/Track B event timeline.
- `summary.json`: gate summary.
- `manifest.json`: runtime config, versions, and file checksums.

Snapshot ID:

```text
{manifest["snapshot_id"]}
```

This is a review fixture, not production MinIO/Kafka/Redis data. Regenerate it
only when the reconstruction contract, selected feature set, seed, or runtime
config changes:

```powershell
powershell -ExecutionPolicy Bypass -File .\\scripts\\stack.ps1 snapshot
```
"""
    path.write_text(text, encoding="utf-8")


def _display_path(path: Path) -> Path:
    """Repo-relative khi output_dir nam trong repo, tuyet doi khi o ngoai
    (vd. --output-dir tro ra ngoai, hay tmp_path trong test)."""
    try:
        return path.relative_to(ROOT)
    except ValueError:
        return path


def build_manifest(
    *,
    result: EndToEndResult,
    config: RuntimeConfig,
    fs: SelectedFeatureSet,
    aliases: BusinessAliasSpec,
    output_dir: Path,
    files: Iterable[Path],
) -> dict[str, Any]:
    file_entries = {}
    for path in sorted(files, key=lambda p: p.name):
        file_entries[path.name] = {
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "snapshot_id": (
            f"{config.version}:{config.semantic_branch}:"
            f"{result.target.target_id}:seed{config.generation_seed}"
        ),
        "description": (
            "Small committed fixture for reviewing deterministic reconstruction "
            "outputs without running the production stack."
        ),
        "target_id": result.target.target_id,
        "customer_id": result.state.customer_id,
        "selected_feature_set_id": fs.id,
        "selected_feature_count": len(fs.columns),
        "business_alias_version": aliases.version,
        "business_alias_semantic_status": aliases.semantic_status,
        "runtime": {
            "config_version": config.version,
            "semantic_status": config.semantic_status,
            "semantic_branch": config.semantic_branch,
            "reference_ts": config.reference_ts.isoformat(),
            "future_days": config.future_days,
            "generation_seed": config.generation_seed,
            "constraint_model_version": config.constraint_model_version,
            "encoding_version": config.encoding_version,
            "objective_version": config.objective_version,
            "selection_policy_version": config.selection_policy_version,
            "solver_version": config.solver_version,
            "behaviour_policy_version": config.behaviour_policy_version,
        },
        "summary": result.summary(),
        "files": file_entries,
        "output_dir": str(_display_path(output_dir)),
    }


def write_snapshot(
    result: EndToEndResult,
    config: RuntimeConfig,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    if not result.all_passed:
        raise ValueError("Refusing to write snapshot because reconstruction gates failed")

    fs = load_feature_set()
    aliases = load_business_aliases()
    output_dir.mkdir(parents=True, exist_ok=True)

    track_a_rows = _track_a_rows(result, aliases)
    track_b_rows = _track_b_rows(result)
    feature_rows = _feature_rows(result, fs, aliases)
    summary = {
        **result.summary(),
        "runtime_config_version": config.version,
        "selected_feature_set_id": fs.id,
        "selected_feature_count": len(fs.columns),
        "business_alias_version": aliases.version,
        "business_alias_semantic_status": aliases.semantic_status,
        "reference_ts": config.reference_ts,
        "generation_seed": config.generation_seed,
        "note": (
            "This committed snapshot is generated once for review. "
            "Production lakehouse data remains outside git."
        ),
    }

    generated_files: list[Path] = []
    track_a_path = output_dir / "track_a_events.csv"
    _write_csv(track_a_path, track_a_rows, (
        "event_id", "customer_id", "target_id", "event_type", "event_family",
        "event_alias", "event_note", "event_ts", "source_type",
        "generation_run_id", "gen_reason", "day_offset", "sub_index",
        "occurrence",
    ))
    generated_files.append(track_a_path)

    track_b_path = output_dir / "track_b_events.csv"
    _write_csv(track_b_path, track_b_rows, (
        "event_id", "customer_id", "event_type", "event_family",
        "event_alias", "event_note", "event_ts", "session_id", "source_type",
        "track", "generation_run_id", "parent_run_id", "root_generation_id",
        "behaviour_policy_version",
    ))
    generated_files.append(track_b_path)

    features_path = output_dir / "features_expected_actual.csv"
    _write_csv(features_path, feature_rows, (
        "column", "business_alias", "display_name", "tier", "method",
        "regime", "source_family", "business_role", "semantic_confidence",
        "reconstruction_note", "expected", "actual", "ok",
    ))
    generated_files.append(features_path)

    feature_svg_path = output_dir / "feature_pass.svg"
    feature_svg_path.write_text(_feature_bar_svg(feature_rows), encoding="utf-8")
    generated_files.append(feature_svg_path)

    timeline_svg_path = output_dir / "timeline.svg"
    timeline_svg_path.write_text(_event_timeline_svg(track_a_rows, track_b_rows), encoding="utf-8")
    generated_files.append(timeline_svg_path)

    summary_path = output_dir / "summary.json"
    _write_json(summary_path, summary)
    generated_files.append(summary_path)

    report_path = output_dir / "report.html"
    _write_html_report(
        report_path,
        result=result,
        config=config,
        fs=fs,
        aliases=aliases,
        feature_rows=feature_rows,
        track_a_rows=track_a_rows,
        track_b_rows=track_b_rows,
    )
    generated_files.append(report_path)

    manifest = build_manifest(
        result=result,
        config=config,
        fs=fs,
        aliases=aliases,
        output_dir=output_dir,
        files=generated_files,
    )
    _write_json(output_dir / "manifest.json", manifest)

    readme_path = output_dir / "README.md"
    _write_snapshot_readme(readme_path, manifest)
    manifest = build_manifest(
        result=result,
        config=config,
        fs=fs,
        aliases=aliases,
        output_dir=output_dir,
        files=[*generated_files, readme_path],
    )
    _write_json(output_dir / "manifest.json", manifest)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--branch", choices=("H1", "H2"), default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    config = load_runtime_config(args.config) if args.config else load_runtime_config()
    if args.branch:
        config = replace(config, semantic_branch=args.branch)

    result = run_demo(config)
    manifest = write_snapshot(result, config, args.output_dir)
    print(json.dumps(_jsonable(manifest), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
