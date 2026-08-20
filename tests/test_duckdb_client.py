"""Unit test cho get_duckdb va duckdb_conn retry logic."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from lzd_pipeline.common.clients import DuckDBWriteLockError, duckdb_conn, get_duckdb


def test_get_duckdb_retries_on_lock_and_succeeds():
    mock_con = MagicMock()
    # Gia lap 2 lan dau bi lock, lan thu 3 thanh cong
    side_effects = [
        Exception("IO Error: Could not set lock on file"),
        Exception("IO Error: Could not set lock on file"),
        mock_con,
    ]

    with patch("duckdb.connect", side_effect=side_effects) as mock_connect, \
         patch("time.sleep") as mock_sleep, \
         patch("lzd_pipeline.common.config.get_settings") as mock_settings:
        mock_settings.return_value.duckdb_path = ":memory:"
        mock_settings.return_value.minio.host_no_scheme = "minio:9000"
        mock_settings.return_value.minio.access_key = "admin"
        mock_settings.return_value.minio.secret_key = "admin123"
        mock_settings.return_value.minio.use_ssl = False

        con = get_duckdb(read_only=True, path=":memory:")
        assert con == mock_con
        assert mock_connect.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(1.0)


def test_get_duckdb_raises_write_lock_error_after_max_retries():
    with patch("duckdb.connect", side_effect=Exception("IO Error: Could not set lock on file")), \
         patch("time.sleep") as mock_sleep, \
         patch("lzd_pipeline.common.config.get_settings") as mock_settings:
        mock_settings.return_value.duckdb_path = ":memory:"

        with pytest.raises(DuckDBWriteLockError):
            get_duckdb(read_only=True, path=":memory:")

        assert mock_sleep.call_count == 4  # max_retries = 5, sleeps 4 times


def test_duckdb_conn_context_manager():
    mock_con = MagicMock()
    with patch("lzd_pipeline.common.clients.get_duckdb", return_value=mock_con):
        with duckdb_conn(read_only=True) as con:
            assert con == mock_con
        mock_con.close.assert_called_once()
