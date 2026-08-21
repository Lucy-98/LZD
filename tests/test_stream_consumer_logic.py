from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from lzd_pipeline.ingestion.stream_consumer import StreamConsumer


class _MissingObject(Exception):
    response = {
        "ResponseMetadata": {"HTTPStatusCode": 404},
        "Error": {"Code": "NoSuchKey"},
    }


class _FakeS3:
    def __init__(self):
        self.objects = {}

    def head_object(self, *, Bucket, Key):  # noqa: N803
        if (Bucket, Key) not in self.objects:
            raise _MissingObject()
        return {"Metadata": self.objects[(Bucket, Key)]["Metadata"]}

    def put_object(self, *, Bucket, Key, Body, Metadata):  # noqa: N803
        self.objects[(Bucket, Key)] = {"Body": Body, "Metadata": Metadata}


def _consumer():
    consumer = StreamConsumer.__new__(StreamConsumer)
    consumer.settings = SimpleNamespace(
        kafka=SimpleNamespace(topic_events="app.user.events.v1"),
        feature_store=SimpleNamespace(
            realtime_future_skew_seconds=300,
            realtime_ttl_seconds=3600,
            realtime_allowed_lateness_seconds=600,
        ),
    )
    consumer.bucket = "lakehouse"
    consumer.s3 = _FakeS3()
    return consumer


def _row(offset: int, partition: int = 0, **overrides):
    row = {
        "event_id": f"e-{partition}-{offset}", "user_id": "U1",
        "event_type": "page_view", "event_ts": 1_800_000_000.0,
        "session_id": "s1", "platform": "web", "item_id": None,
        "category_id": None, "price": 0.0, "quantity": 0,
        "schema_version": 1, "ingested_at": 1_800_000_001.0,
        "kafka_partition": partition, "kafka_offset": offset,
    }
    row.update(overrides)
    return row


def test_lake_object_identity_uses_partition_and_offset_range():
    consumer = _consumer()
    keys = consumer._write_parquet([_row(11), _row(10), _row(7, partition=1)])
    assert len(keys) == 2
    assert any("partition=0/offset-10-11.parquet" in key for key in keys)
    assert any("partition=1/offset-7-7.parquet" in key for key in keys)


def test_replaying_same_offset_range_does_not_create_another_object():
    consumer = _consumer()
    rows = [_row(10), _row(11)]
    first = consumer._write_parquet(rows)
    second = consumer._write_parquet(rows)
    assert first == second
    assert len(consumer.s3.objects) == 1


def test_same_offset_range_with_different_payload_fails_closed():
    consumer = _consumer()
    consumer._write_parquet([_row(10)])
    with pytest.raises(RuntimeError, match="idempotency conflict"):
        consumer._write_parquet([_row(10, event_type="order")])


def test_watermark_drops_expired_and_future_events(monkeypatch):
    consumer = _consumer()
    now = time.time()

    class Store:
        def __init__(self):
            self.events = []

        def apply_realtime_event(self, *args, **kwargs):
            self.events.append((args, kwargs))
            return True

    consumer.store = Store()
    result = consumer._update_realtime([
        _row(1, event_ts=now - 3700),
        _row(2, event_ts=now + 301),
        _row(3, event_ts=now - 700),
    ])
    assert result["late_dropped"] == 1
    assert result["future_dropped"] == 1
    assert result["late_accepted"] == 1
    assert result["applied"] == 1
    assert len(consumer.store.events) == 1


def test_dlq_offset_is_only_committed_when_no_valid_rows_are_buffered():
    consumer = _consumer()
    commits = []
    consumer.consumer = SimpleNamespace(
        commit=lambda **kwargs: commits.append(kwargs)
    )
    message = object()
    consumer.buffer = []
    consumer._commit_invalid_if_safe(message, delivered=True)
    assert commits == [{"message": message, "asynchronous": False}]

    consumer.buffer = [_row(1)]
    consumer._commit_invalid_if_safe(message, delivered=True)
    consumer._commit_invalid_if_safe(message, delivered=False)
    assert len(commits) == 1
