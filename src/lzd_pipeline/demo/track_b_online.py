"""Demo 10 user: Track A -> 55F -> Track B -> Kafka/MinIO/Redis/FastAPI.

Day la duong demo CO GIOI HAN, khong phai producer traffic chay lien tuc:

1. Chon N dong train, reconstruction Track A thanh artifact.
2. Land raw Track A vao MinIO + control data, chay dbt that vao DuckDB.
3. Sync mart 55F tu DuckDB sang Redis bang engine sync production.
4. Ban giao CustomerState(T0), sinh Track B synthetic gan thoi diem demo.
5. Adapter Track B -> AppEvent v1, publish Kafka va cho consumer flush.
6. Xac minh MinIO, stg_app_events, Redis overlay va FastAPI/model trong image.

Track B chi nhan CustomerState; policy demo khong doc target/55 feature. Event
Track A (`EVT_*`) khong bao gio duoc publish vao topic app online.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence
from urllib.error import URLError
from urllib.request import Request, urlopen

from lzd_pipeline.common.clients import (
    duckdb_writer,
    get_duckdb,
    get_kafka_producer,
    get_pg_connection,
    get_s3_client,
)
from lzd_pipeline.common.config import get_settings
from lzd_pipeline.features.online_store import OnlineFeatureStore
from lzd_pipeline.features.spec import load_feature_spec
from lzd_pipeline.ingestion.schemas import AppEvent, validate_event
from lzd_pipeline.reconstruction import runner
from lzd_pipeline.reconstruction.e2e import RuntimeConfig, load_runtime_config
from lzd_pipeline.reconstruction.feature_set import load_feature_set
from lzd_pipeline.reconstruction.handoff import GateReport, to_customer_state
from lzd_pipeline.reconstruction.live import live_generator
from lzd_pipeline.reconstruction.semantics import build_branch
from lzd_pipeline.reconstruction.sink import land_track_a
from lzd_pipeline.reconstruction.state import CustomerState, Provenance
from lzd_pipeline.reconstruction.track_a_batch import (
    DEFAULT_INPUT,
    _iter_rows,
    fit_value_maps,
    lzd_user_id,
    materialize_track_a,
    reconstruct_row,
)
from lzd_pipeline.reconstruction.warehouse import create_biz_shell
from lzd_pipeline.serving.campaign_policy import (
    SEND_VOUCHER,
    SUPPRESS_ALREADY_PURCHASED,
    WAIT_FOR_INTENT,
)


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = ROOT / ".tmp" / "track_b_online_demo"
DBT_DIR = ROOT / "dbt"
TRACK_A_SERVING_TABLE = "marts.feat_cfs_reconstructed_user_serving"
TRACK_B_TO_APP = {
    "SESSION_STARTED": "app_open",
    "PRODUCT_VIEWED": "page_view",
    "ITEM_ADDED_TO_CART": "add_to_cart",
    "PURCHASE_COMPLETED": "order",
}


@dataclass(frozen=True)
class DemoUser:
    target_id: str
    user_id: str
    track_a_events: int
    state: CustomerState


@dataclass(frozen=True)
class DemoOnlineBehaviour:
    """Track B bounded voi rt_* synthetic nhin thay duoc khi demo.

    Scenario chi phu thuoc identity cua CustomerState, khong doc target/55F/
    uplift score. Voi cohort U0000000..U0000009, no co chu y tao du ca ba
    timing state: co cart intent, chua co intent, va da order.
    """

    policy_version: str = "demo_online_rt_scenario_v2"

    def next_events(
        self,
        state: CustomerState,
        t_from: datetime,
        t_to: datetime,
        rng_seed: int,
    ) -> Iterator[tuple[str, datetime]]:
        # Fixture theo identity de rerun cho cung rt_*; khong chon scenario
        # bang model score. Thay cart bang page-view cho hai user "cho intent"
        # nhung van giu 4 base event/user. Nam user co order => cohort 10 user
        # van tao dung 45 event, de audit/report cu so sanh duoc.
        digits = "".join(ch for ch in state.customer_id if ch.isdigit())
        ordinal = int(digits or "0") % 10
        waiting_for_intent = ordinal in {1, 6}
        already_purchased = ordinal in {0, 3, 4, 7, 9}
        sequence = [
            ("SESSION_STARTED", 20),
            ("PRODUCT_VIEWED", 80),
            ("PRODUCT_VIEWED", 150),
            (
                "PRODUCT_VIEWED" if waiting_for_intent else "ITEM_ADDED_TO_CART",
                230,
            ),
        ]
        if already_purchased:
            sequence.append(("PURCHASE_COMPLETED", 320))
        for kind, seconds in sequence:
            ts = t_from + timedelta(seconds=seconds)
            if ts < t_to:
                yield kind, ts


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _api_json(
    base_url: str,
    path: str,
    *,
    payload: Mapping[str, Any] | None = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = Request(
        base_url.rstrip("/") + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST" if data is not None else "GET",
    )
    with urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_api(base_url: str, timeout: float) -> dict[str, Any]:
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            return _api_json(base_url, "/health", timeout=5)
        except (OSError, URLError, ValueError) as exc:
            last = exc
            time.sleep(1)
    raise TimeoutError(f"FastAPI chua san sang sau {timeout}s: {last}")


def _postgres_track_a_reusable(output_dir: Path) -> bool:
    """Cho phep rerun demo neu chinh target immutable do da land truoc do.

    Partial/mismatch van fail cung — khong co `ON CONFLICT DO NOTHING` che
    mot lan reconstruction khac. Ham chi reuse khi toan bo target/hash va
    sidecar theo target deu ton tai dung so luong.
    """
    target_path = output_dir / "biz_reconstruction_target.csv"
    with target_path.open(newline="", encoding="utf-8") as fh:
        targets = list(csv.DictReader(fh))
    expected = {
        row["target_id"]: (row["target_hash"], row["feature_payload_hash"])
        for row in targets
    }
    if not expected:
        raise RuntimeError("Track A khong co target nao de land")

    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT target_id, target_hash, feature_payload_hash "
                "FROM biz.reconstruction_target WHERE target_id = ANY(%s)",
                (list(expected),),
            )
            actual = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
            if not actual:
                return False
            if actual != expected:
                raise RuntimeError(
                    "Postgres co Track A partial/khac hash; khong duoc reuse target immutable"
                )

            expected_attrs = sum(
                1
                for _ in csv.DictReader(
                    (output_dir / "biz_customer_attribute.csv").open(
                        newline="", encoding="utf-8"
                    )
                )
            )
            cur.execute(
                "SELECT count(*) FROM biz.customer_attribute WHERE target_id = ANY(%s)",
                (list(expected),),
            )
            actual_attrs = int(cur.fetchone()[0])
            cur.execute(
                "SELECT count(*) FROM biz.passthrough_source WHERE target_id = ANY(%s)",
                (list(expected),),
            )
            actual_passthrough = int(cur.fetchone()[0])
    finally:
        conn.close()

    if actual_attrs != expected_attrs or actual_passthrough != len(expected):
        raise RuntimeError(
            "Postgres Track A target co nhung sidecar thieu; khong duoc reuse"
        )
    return True


def _run_track_a_dbt(database_path: Path, logical_date: str) -> dict[str, Any]:
    # Ban demo cu tung tao index bang post_hook. Trong DuckDB, index la mot
    # dependency lam dbt khong the replace table o lan rerun. Drop migration
    # nay la no-op cho database moi; model hien tai co y khong tao lai index.
    con = get_duckdb(read_only=False, path=str(database_path))
    try:
        con.execute("DROP INDEX IF EXISTS marts.idx_cfs_user_serving_user")
    finally:
        con.close()

    env = {
        **os.environ,
        "DBT_PROFILES_DIR": str(DBT_DIR),
        "DUCKDB_PATH": str(database_path),
    }
    variables = json.dumps({"run_date": logical_date})
    for action in ("run", "test"):
        subprocess.run(
            [
                "dbt", action, "--no-version-check",
                "--select", "tag:reconstruction",
                "--vars", variables,
            ],
            cwd=DBT_DIR,
            env=env,
            check=True,
        )

    fs = load_feature_set()
    with get_duckdb(read_only=True, path=str(database_path)) as con:
        rows = int(con.execute(
            f"SELECT count(*) FROM {TRACK_A_SERVING_TABLE}"
        ).fetchone()[0])
        columns = [
            row[0] for row in con.execute(
                f"DESCRIBE {TRACK_A_SERVING_TABLE}"
            ).fetchall()
        ]
    actual_features = [name for name in columns if name in fs.columns]
    if set(actual_features) != set(fs.columns) or len(actual_features) != 55:
        raise RuntimeError(
            f"{TRACK_A_SERVING_TABLE} khong dung 55F: {actual_features}"
        )
    return {
        "selector": "tag:reconstruction",
        "run": "PASS",
        "test": "PASS",
        "mart": TRACK_A_SERVING_TABLE,
        "rows": rows,
        "feature_columns": len(actual_features),
    }


def build_track_a_lakehouse(
    *,
    database_path: Path,
    input_path: Path,
    users: int,
    config: RuntimeConfig,
    output_dir: Path,
) -> tuple[dict[str, Any], list[DemoUser]]:
    """Track A artifact -> MinIO/control -> dbt -> DuckDB mart 55F."""
    fs = load_feature_set()
    track_a_dir = output_dir / "track_a"
    manifest = materialize_track_a(
        input_path=input_path,
        output_dir=track_a_dir,
        limit=users,
        config=config,
        verify_limit=users,
    )
    if manifest["solved_rows"] != users or not manifest["gate_sample"]["all_passed"]:
        raise RuntimeError(f"Track A artifact/gate khong dat: {manifest}")

    logical_date = config.reference_ts.date().isoformat()
    postgres_reused = _postgres_track_a_reusable(track_a_dir)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb_writer() as con:
        create_biz_shell(con, fs)
        landed = land_track_a(
            track_a_dir,
            logical_date,
            con=con,
            to_postgres=not postgres_reused,
            to_lake=True,
        )
    if postgres_reused:
        landed["postgres"] = {
            "status": "REUSED_VERIFIED",
            "targets": users,
        }

    dbt_report = _run_track_a_dbt(database_path, logical_date)
    if dbt_report["rows"] != users:
        raise RuntimeError(
            f"dbt Track A co {dbt_report['rows']} rows, expected={users}"
        )

    source_rows = list(_iter_rows(input_path, users))
    if len(source_rows) != users:
        raise RuntimeError(f"can {users} user nhung input chi co {len(source_rows)}")
    value_maps = fit_value_maps(input_path, fs, limit=users)
    reconstructed = [
        reconstruct_row(row, fs=fs, value_maps=value_maps, config=config)
        for row in source_rows
    ]
    with get_duckdb(read_only=True, path=str(database_path)) as con:
        col_sql = ", ".join(f'"{name}"' for name in fs.columns)
        cursor = con.execute(
            "SELECT target_id, " + col_sql + " "
            "FROM marts.feat_cfs_reconstructed_selected"
        )
        actual_by_target = {
            str(row[0]): dict(zip(fs.columns, row[1:]))
            for row in cursor.fetchall()
        }

        branch = build_branch(config.semantic_branch)
        demo_users: list[DemoUser] = []
        for item in reconstructed:
            actual = actual_by_target.get(item.target.target_id, {})
            gate_a = runner.gate_a(item.target.values, actual, fs)
            gate_b = bool(item.outcome.events) and all(
                event.event_ts < item.target.reference_ts
                for event in item.outcome.events
            )
            gate_c = (
                item.outcome.candidate is not None
                and branch.is_feasible(item.outcome.candidate, item.decoded)
            )
            gates = GateReport(
                gate_a=gate_a.gate_a_pass,
                gate_a_t3=gate_a.gate_a_t3_pass,
                gate_b=gate_b,
                gate_c=gate_c,
            )
            provenance = Provenance(
                source_type="RECONSTRUCTED",
                generation_run_id=item.generation_run_id,
                root_generation_id=item.generation_run_id,
                parent_target_id=item.target.target_id,
                parent_entity_id=lzd_user_id(item.target.target_id),
                parent_feature_version=item.target.feature_version,
                created_at=item.target.reference_ts,
            )
            state = to_customer_state(
                item.outcome,
                item.decoded,
                gates,
                customer_id=lzd_user_id(item.target.target_id),
                target_id=item.target.target_id,
                reference_ts=item.target.reference_ts,
                attributes=dict(item.attributes),
                semantic_branch=config.semantic_branch,  # type: ignore[arg-type]
                semantic_status=config.semantic_status,
                provenance=provenance,
            )
            demo_users.append(DemoUser(
                target_id=item.target.target_id,
                user_id=state.customer_id,
                track_a_events=len(item.outcome.events),
                state=state,
            ))

    if len(demo_users) != users or not all(user.state for user in demo_users):
        raise RuntimeError("Track A handoff khong du so user")
    final_gates = [
        runner.gate_a(
            item.target.values,
            actual_by_target[item.target.target_id],
            fs,
        )
        for item in reconstructed
    ]
    if not all(gate.gate_a_pass and gate.gate_a_t3_pass for gate in final_gates):
        raise RuntimeError("Gate A tren output dbt production bi fail")
    return {
        "logical_date": logical_date,
        "manifest": manifest,
        "landed": landed,
        "dbt": dbt_report,
    }, demo_users


def sync_track_a_features(logical_date: str) -> dict[str, Any]:
    """Dung engine production de sync mart dbt Track A sang Redis."""
    # Lazy import giu module demo/test adapter nhe; image runtime co day du
    # prometheus/psycopg, con host dev toi thieu van test duoc event contract.
    from lzd_pipeline.features import sync as feature_sync

    prepared = feature_sync.prepare_sync(
        logical_date, shards=1, table=TRACK_A_SERVING_TABLE
    )
    shard = feature_sync.sync_shard(
        logical_date, shard_id=0, shards=1, table=TRACK_A_SERVING_TABLE
    )
    validation = feature_sync.validate_sync(
        logical_date, sample_size=prepared["expected_rows"],
        table=TRACK_A_SERVING_TABLE,
    )
    activated = feature_sync.activate_version(logical_date)
    return {
        "version": prepared["version"],
        "source_table": TRACK_A_SERVING_TABLE,
        "users": prepared["expected_rows"],
        "feature_columns": 55,
        "validated_values": prepared["expected_rows"] * 55,
        "shard": shard,
        "validation": validation,
        "activated": activated,
    }


def sync_demo_batch(
    rows: Sequence[dict[str, Any]],
    *,
    version: str,
    store: OnlineFeatureStore | None = None,
) -> dict[str, Any]:
    """Sync va validate toan bo N x 55 truoc khi activate demo version."""
    store = store or OnlineFeatureStore()
    spec = store.spec
    if not rows:
        raise ValueError("khong co row de sync")

    user_ids = [str(row[spec.entity_key]) for row in rows]
    for row in rows:
        missing = [name for name in spec.batch_names if row.get(name) is None]
        if missing:
            raise RuntimeError(f"{row.get(spec.entity_key)} thieu 55F: {missing}")

    # Namespace version demo duoc ghi lai idempotent; chi xoa dung 10 key dich.
    keys = [spec.batch_key(version, user_id) for user_id in user_ids]
    if keys:
        store.r.delete(*keys)
    store.set_version_status(
        version,
        status="IN_PROGRESS",
        expected_rows=len(rows),
        total_shards=1,
        demo="track_b_online",
        started_at=int(time.time()),
    )
    written = store.write_rows(version, rows)
    if written != len(rows):
        raise RuntimeError(f"Redis chi ghi {written}/{len(rows)} user")

    mismatches: list[str] = []
    for row in rows:
        user_id = str(row[spec.entity_key])
        raw, _ = store.get_features(user_id, version=version)
        for name in spec.batch_names:
            if name not in raw:
                mismatches.append(f"{user_id}:{name}:missing")
                continue
            try:
                if float(raw[name]) != float(row[name]):
                    mismatches.append(f"{user_id}:{name}:value")
            except (TypeError, ValueError):
                mismatches.append(f"{user_id}:{name}:type")
    if mismatches:
        store.set_version_status(version, status="FAILED", error=mismatches[:10])
        raise RuntimeError(f"Redis lech DuckDB: {mismatches[:10]}")

    previous = store.activate_version(version)
    return {
        "version": version,
        "previous_version": previous,
        "users": len(rows),
        "feature_columns": len(spec.batch_names),
        "validated_values": len(rows) * len(spec.batch_names),
    }


def generate_track_b_app_events(
    users: Sequence[DemoUser],
    *,
    anchor: datetime,
    seed: int,
) -> tuple[list[AppEvent], list[dict[str, Any]]]:
    """Sinh Track B recent va adapter sang schema AppEvent v1."""
    if anchor.tzinfo is None:
        raise ValueError("anchor phai co timezone")
    policy = DemoOnlineBehaviour()
    t_from = anchor - timedelta(minutes=9)
    t_to = anchor - timedelta(seconds=5)
    app_events: list[AppEvent] = []
    lineage: list[dict[str, Any]] = []

    for user in users:
        future = tuple(live_generator(
            user.state,
            policy,
            t_from,
            t_to,
            rng_seed=seed,
        ))
        if not future:
            raise RuntimeError(f"Track B khong sinh event cho {user.user_id}")
        for event in future:
            event_type = TRACK_B_TO_APP[event.event_type]
            is_order = event_type == "order"
            app_event = AppEvent(
                event_id=event.event_id,
                user_id=user.user_id,
                event_type=event_type,
                event_ts=event.event_ts.timestamp(),
                session_id=event.session_id,
                platform="web",
                item_id=f"demo_item_{user.user_id[-4:]}",
                category_id="demo_track_b",
                price=199_000.0 if is_order else 0.0,
                quantity=1 if is_order else 0,
            )
            ok, reason = validate_event(app_event.to_dict())
            if not ok:
                raise RuntimeError(f"adapter tao AppEvent sai: {reason}")
            app_events.append(app_event)
            lineage.append({
                "event_id": event.event_id,
                "user_id": user.user_id,
                "parent_target_id": user.target_id,
                "track": event.track,
                "source_type": event.source_type,
                "track_b_event_type": event.event_type,
                "app_event_type": event_type,
                "event_ts": event.event_ts.isoformat(),
                "generation_run_id": event.generation_run_id,
                "root_generation_id": event.provenance.root_generation_id,
                "parent_run_id": event.provenance.parent_run_id,
                "behaviour_policy_version": event.provenance.behaviour_policy_version,
            })
    return app_events, lineage


def expected_realtime(events: Sequence[AppEvent]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for event in events:
        row = out.setdefault(event.user_id, {
            "rt_events_1h": 0.0,
            "rt_page_view_1h": 0.0,
            "rt_add_to_cart_1h": 0.0,
            "rt_order_1h": 0.0,
            "rt_gmv_1h": 0.0,
        })
        row["rt_events_1h"] += 1
        if event.event_type == "page_view":
            row["rt_page_view_1h"] += 1
        elif event.event_type == "add_to_cart":
            row["rt_add_to_cart_1h"] += 1
        elif event.event_type == "order":
            row["rt_order_1h"] += 1
            row["rt_gmv_1h"] += event.price * event.quantity
    return out


def clear_demo_realtime(user_ids: Sequence[str], store: OnlineFeatureStore) -> int:
    keys = [store.spec.realtime_key(user_id) for user_id in user_ids]
    return int(store.r.delete(*keys)) if keys else 0


def publish_app_events(events: Sequence[AppEvent]) -> dict[str, Any]:
    settings = get_settings()
    producer = get_kafka_producer()
    failures: list[str] = []

    def delivered(err, msg) -> None:
        if err is not None:
            failures.append(str(err))

    for event in events:
        producer.produce(
            settings.kafka.topic_events,
            key=event.user_id,
            value=json.dumps(event.to_dict()).encode("utf-8"),
            on_delivery=delivered,
        )
        producer.poll(0)
    remaining = producer.flush(20)
    if failures or remaining:
        raise RuntimeError(
            f"Kafka publish loi: failures={failures[:5]}, remaining={remaining}"
        )
    return {"topic": settings.kafka.topic_events, "events": len(events)}


def wait_realtime(
    expected: Mapping[str, Mapping[str, float]],
    *,
    anchor: datetime,
    store: OnlineFeatureStore,
    timeout: float,
) -> dict[str, dict[str, Any]]:
    deadline = time.time() + timeout
    last: dict[str, dict[str, Any]] = {}
    while time.time() < deadline:
        last = {}
        passed = True
        for user_id, wanted in expected.items():
            _, raw = store.get_features(user_id)
            actual = store.aggregate_realtime(raw, now=anchor.timestamp())
            last[user_id] = actual
            for name, value in wanted.items():
                # Counter chua tung phat sinh khong duoc ghi vao Redis; ve
                # nghiep vu field vang mat tuong duong 0 (vd user khong order).
                if float(actual.get(name, 0.0)) != float(value):
                    passed = False
        if passed:
            return last
        time.sleep(1)
    raise TimeoutError(f"Redis realtime chua khop sau {timeout}s: {last}")


def wait_minio_event_ids(event_ids: set[str], timeout: float) -> list[str]:
    import pyarrow.parquet as pq

    settings = get_settings()
    s3 = get_s3_client()
    deadline = time.time() + timeout
    matched_keys: set[str] = set()
    found: set[str] = set()

    while time.time() < deadline:
        token: str | None = None
        objects: list[dict[str, Any]] = []
        while True:
            kwargs: dict[str, Any] = {
                "Bucket": settings.minio.bucket_lake,
                "Prefix": "raw/app_events/",
            }
            if token:
                kwargs["ContinuationToken"] = token
            page = s3.list_objects_v2(**kwargs)
            objects.extend(page.get("Contents", []))
            if not page.get("IsTruncated"):
                break
            token = page.get("NextContinuationToken")

        for obj in objects:
            key = str(obj["Key"])
            if key in matched_keys or not key.endswith(".parquet"):
                continue
            body = s3.get_object(
                Bucket=settings.minio.bucket_lake, Key=key
            )["Body"].read()
            table = pq.read_table(io.BytesIO(body), columns=["event_id"])
            ids = {str(value) for value in table.column("event_id").to_pylist()}
            overlap = ids & event_ids
            if overlap:
                found |= overlap
                matched_keys.add(key)
        if found == event_ids:
            return sorted(matched_keys)
        time.sleep(1)
    raise TimeoutError(
        f"MinIO moi co {len(found)}/{len(event_ids)} event demo sau {timeout}s"
    )


def build_and_verify_staging(
    event_ids: Sequence[str], *, database_path: Path
) -> dict[str, Any]:
    dbt_dir = Path(os.environ.get("DBT_PROJECT_DIR", ROOT / "dbt"))
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": str(dbt_dir),
        "DUCKDB_PATH": str(database_path),
    }
    subprocess.run(
        ["dbt", "run", "--no-version-check", "--select", "stg_app_events"],
        cwd=dbt_dir,
        env=env,
        check=True,
    )
    placeholders = ", ".join("?" for _ in event_ids)
    # View staging doc Parquet truc tiep tu MinIO. Connection moi cung phai
    # nap httpfs + endpoint/credential S3 giong dbt; duckdb.connect() tran se
    # mac dinh goi AWS endpoint va tra HTTP 400.
    with get_duckdb(read_only=True, path=str(database_path)) as con:
        rows, unique_rows = con.execute(
            "SELECT count(*), count(distinct event_id) "
            "FROM staging.stg_app_events "
            f"WHERE event_id IN ({placeholders})",
            list(event_ids),
        ).fetchone()
    if rows != len(event_ids) or unique_rows != len(event_ids):
        raise RuntimeError(
            f"staging co rows={rows}, unique={unique_rows}, expected={len(event_ids)}"
        )
    return {"relation": "staging.stg_app_events", "rows": rows, "deduped": True}


def _score_batch(base_url: str, user_ids: Sequence[str]) -> dict[str, Any]:
    return _api_json(
        base_url,
        "/decide/batch",
        payload={"user_ids": list(user_ids), "context": {}},
        timeout=30,
    )


def verify_api(
    base_url: str,
    user_ids: Sequence[str],
    before: Mapping[str, Any],
) -> dict[str, Any]:
    store_info = _api_json(base_url, "/store/info")
    after = _score_batch(base_url, user_ids)
    campaign = _api_json(
        base_url,
        "/campaign/decide",
        payload={
            "user_ids": list(user_ids),
            "budget": min(3, len(user_ids)),
            "context": {},
        },
        timeout=30,
    )
    features = [_api_json(base_url, f"/features/{user_id}") for user_id in user_ids]

    before_rows = {row["user_id"]: row for row in before["results"]}
    after_rows = {row["user_id"]: row for row in after["results"]}
    for user_id in user_ids:
        row = after_rows[user_id]
        if not row["cache_hit"] or row["features_supplied"] != 55:
            raise RuntimeError(f"FastAPI khong nhan du 55F cho {user_id}: {row}")
        if row["realtime_applied"] != 0:
            raise RuntimeError("model hien tai khong khai rt_* nhung API bao da ap dung")
        if row["uplift_score"] != before_rows[user_id]["uplift_score"]:
            raise RuntimeError(
                "score doi sau Track B du model khong co rt_* trong contract"
            )
    campaign_rows = {row["user_id"]: row for row in campaign["results"]}
    if set(campaign_rows) != set(user_ids):
        raise RuntimeError("campaign response khong du cohort user")
    for user_id in user_ids:
        row = campaign_rows[user_id]
        if row["uplift_score"] != after_rows[user_id]["uplift_score"]:
            raise RuntimeError("campaign policy da sua uplift score cua model")
        if row["realtime_applied_to_model"] != 0:
            raise RuntimeError("campaign bao rt_* da vao model artifact hien tai")
    if campaign["policy"].get("model_score_uses_realtime") is not False:
        raise RuntimeError("campaign khong noi ro realtime chi la timing policy")
    if len(user_ids) >= 10:
        actions = set(campaign["action_counts"])
        expected_actions = {
            SEND_VOUCHER, WAIT_FOR_INTENT, SUPPRESS_ALREADY_PURCHASED,
        }
        if not expected_actions <= actions:
            raise RuntimeError(
                f"rt_* synthetic chua tao du timing state: {campaign['action_counts']}"
            )
    if store_info.get("model", {}).get("source") != "docker_image":
        raise RuntimeError(f"API khong dung model trong image: {store_info}")
    if not all(item["n_realtime_fields"] > 0 for item in features):
        raise RuntimeError("API chua nhin thay Redis realtime cua du 10 user")
    return {
        "store_info": store_info,
        "before_track_b": before,
        "after_track_b": after,
        "campaign": campaign,
        "feature_debug": [{
            "user_id": item["user_id"],
            "cache_hit": item["cache_hit"],
            "n_batch_fields": item["n_batch_fields"],
            "n_realtime_fields": item["n_realtime_fields"],
            "realtime_window": item["realtime_window"],
        } for item in features],
        "expected_limitation": (
            "Track B cap nhat rt_* nhung DRLearner artifact hien tai chi nhan "
            "55 batch feature; realtime_applied=0 va score khong doi. rt_* chi "
            "gate SEND/WAIT/SUPPRESS trong campaign policy synthetic."
        ),
    }


def run_demo(args: argparse.Namespace) -> dict[str, Any]:
    settings = get_settings()
    spec = load_feature_spec()
    if len(spec.batch_names) != 55:
        raise RuntimeError(f"demo can dung 55 batch feature, spec co {len(spec.batch_names)}")

    anchor = (
        datetime.fromisoformat(args.anchor.replace("Z", "+00:00"))
        if args.anchor else datetime.now(timezone.utc).replace(microsecond=0)
    )
    if anchor.tzinfo is None:
        anchor = anchor.replace(tzinfo=timezone.utc)
    run_id = args.run_id or f"track-b-demo-{anchor:%Y%m%dT%H%M%SZ}"
    output_dir = args.output_dir / run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    database_path = Path(settings.duckdb_path)
    runtime_config = load_runtime_config()

    track_a, cohort = build_track_a_lakehouse(
        database_path=database_path,
        input_path=args.input,
        users=args.users,
        config=runtime_config,
        output_dir=output_dir,
    )
    user_ids = [user.user_id for user in cohort]
    store = OnlineFeatureStore()
    cleared = clear_demo_realtime(user_ids, store)
    sync = sync_track_a_features(track_a["logical_date"])
    if store.get_active_version() != sync["version"]:
        raise RuntimeError("Redis active_version chua tro vao 55F Track A")

    wait_api(args.api_url, args.timeout)
    before = _score_batch(args.api_url, user_ids)

    events, lineage = generate_track_b_app_events(
        cohort, anchor=anchor, seed=args.seed
    )
    event_path = output_dir / "track_b_app_events.jsonl"
    with event_path.open("w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")
    lineage_path = output_dir / "track_b_lineage.json"
    lineage_path.write_text(
        json.dumps(_jsonable(lineage), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    kafka = publish_app_events(events)
    realtime = wait_realtime(
        expected_realtime(events),
        anchor=anchor,
        store=store,
        timeout=args.timeout,
    )
    minio_keys = wait_minio_event_ids(
        {event.event_id for event in events}, args.timeout
    )
    staging = build_and_verify_staging(
        [event.event_id for event in events], database_path=database_path
    )
    api = verify_api(args.api_url, user_ids, before)

    report = {
        "run_id": run_id,
        "anchor": anchor,
        "cohort": [{
            "target_id": user.target_id,
            "user_id": user.user_id,
            "track_a_events": user.track_a_events,
        } for user in cohort],
        "track_a": {
            "users": len(cohort),
            "events": sum(user.track_a_events for user in cohort),
            "minio": track_a["landed"]["lake"],
            "control_plane": track_a["landed"]["postgres"],
            "duckdb_control": track_a["landed"]["duckdb_verified"],
            "dbt": track_a["dbt"],
            "mart": TRACK_A_SERVING_TABLE,
            "feature_columns": 55,
            "all_gates_passed": True,
        },
        "feature_sync": sync,
        "realtime_keys_cleared_before_demo": cleared,
        "track_b": {
            "events": len(events),
            "source_type": "SYNTHETIC",
            "policy": DemoOnlineBehaviour.policy_version,
            "kafka": kafka,
        },
        "minio": {
            "track_a": track_a["landed"]["lake"],
            "track_b_keys": minio_keys,
            "matched_events": len(events),
        },
        "staging": staging,
        "redis_realtime": realtime,
        "api": api,
        "artifacts": {
            "events": str(event_path),
            "lineage": str(lineage_path),
            "report": str(output_dir / "report.json"),
        },
    }
    report_path = output_dir / "report.json"
    report_path.write_text(
        json.dumps(_jsonable(report), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--users", type=int, default=10)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--run-id")
    parser.add_argument("--anchor", help="ISO timestamp; mac dinh = UTC now")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument(
        "--api-url", default=os.environ.get("DEMO_API_URL", "http://inference-api:8000")
    )
    args = parser.parse_args()
    if args.users < 1 or args.users > 100:
        raise SystemExit("--users phai trong [1, 100]")

    report = run_demo(args)
    print(json.dumps({
        "run_id": report["run_id"],
        "users": report["track_a"]["users"],
        "track_a_events": report["track_a"]["events"],
        "track_b_events": report["track_b"]["events"],
        "feature_columns": report["feature_sync"]["feature_columns"],
        "minio_events": report["minio"]["matched_events"],
        "staging_rows": report["staging"]["rows"],
        "api_results": report["api"]["after_track_b"]["count"],
        "report": report["artifacts"]["report"],
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
