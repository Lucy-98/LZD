from unittest.mock import MagicMock, patch

from lzd_pipeline.common import audit


def test_record_skipped_shard_preserves_previous_row_count():
    cursor = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = cursor

    with patch.object(audit, "pg_cursor", return_value=context):
        audit.record_shard(
            "v20260801", 3, "DONE", rows_written=0, preserve_rows=True
        )

    upsert_sql, params = cursor.execute.call_args_list[0].args
    assert "WHEN %s THEN ops.feature_sync_shard.rows_written" in upsert_sql
    assert params[-1] is True


def test_record_normal_shard_replaces_row_count():
    cursor = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = cursor

    with patch.object(audit, "pg_cursor", return_value=context):
        audit.record_shard("v20260801", 3, "DONE", rows_written=157)

    _, params = cursor.execute.call_args_list[0].args
    assert params[3] == 157
    assert params[-1] is False


def test_validation_can_repair_audit_row_count_from_redis():
    cursor = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = cursor

    with patch.object(audit, "pg_cursor", return_value=context):
        audit.set_sync_status("v20260801", "VALIDATED", written_rows=5000)

    sql, params = cursor.execute.call_args.args
    assert "written_rows = COALESCE(%s, written_rows)" in sql
    assert 5000 in params
