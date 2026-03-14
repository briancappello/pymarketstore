"""Tests for pymarketstore.cli — Click CLI commands and helper functions."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from click.testing import CliRunner

from pymarketstore.cli import (
    _format_timestamp,
    _is_daily_or_higher,
    _output_dataframe,
    _timeframe_sort_key,
    cli,
)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


class TestIsDailyOrHigher:
    def test_daily(self):
        assert _is_daily_or_higher("1D") is True

    def test_weekly(self):
        assert _is_daily_or_higher("1W") is True

    def test_monthly(self):
        assert _is_daily_or_higher("1M") is True
        assert _is_daily_or_higher("3M") is True

    def test_yearly(self):
        assert _is_daily_or_higher("1Y") is True

    def test_minute(self):
        assert _is_daily_or_higher("1Min") is False

    def test_hour(self):
        assert _is_daily_or_higher("1H") is False

    def test_second(self):
        assert _is_daily_or_higher("1Sec") is False


class TestTimeframeSortKey:
    def test_sorts_by_duration(self):
        timeframes = ["1D", "1Min", "1H", "5Min", "1Sec", "1W"]
        sorted_tfs = sorted(timeframes, key=_timeframe_sort_key)
        assert sorted_tfs == ["1Sec", "1Min", "5Min", "1H", "1D", "1W"]

    def test_same_unit_sorts_by_number(self):
        timeframes = ["30Min", "5Min", "1Min", "15Min", "10Min"]
        sorted_tfs = sorted(timeframes, key=_timeframe_sort_key)
        assert sorted_tfs == ["1Min", "5Min", "10Min", "15Min", "30Min"]

    def test_unknown_unit_sorts_last(self):
        assert _timeframe_sort_key("unknown")[0] == 999


class TestFormatTimestamp:
    def test_daily_shows_date_only(self):
        ts = pd.Timestamp("2024-01-15 09:30:00", tz="UTC")
        result = _format_timestamp(ts, "1D")
        assert result == "2024-01-15"

    def test_weekly_shows_date_only(self):
        ts = pd.Timestamp("2024-01-15", tz="UTC")
        result = _format_timestamp(ts, "1W")
        assert result == "2024-01-15"

    def test_intraday_shows_datetime_in_ny(self):
        ts = pd.Timestamp("2024-01-15 14:30:00", tz="UTC")
        result = _format_timestamp(ts, "1Min")
        # 14:30 UTC = 09:30 ET
        assert result == "2024-01-15 09:30"


class TestOutputDataframe:
    def test_csv_format(self, capsys):
        df = pd.DataFrame({"Open": [100.0], "Close": [101.0]})
        _output_dataframe(df, "csv")
        captured = capsys.readouterr()
        assert "Open" in captured.out
        assert "100.0" in captured.out

    def test_json_format(self, capsys):
        df = pd.DataFrame({"Open": [100.0]})
        _output_dataframe(df, "json")
        captured = capsys.readouterr()
        assert "Open" in captured.out

    def test_table_format(self, capsys):
        df = pd.DataFrame({"Open": [100.0]})
        _output_dataframe(df, "table")
        captured = capsys.readouterr()
        assert "Open" in captured.out


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


class TestVersionCommand:
    @patch("pymarketstore.cli.pymkts.Client")
    def test_version_outputs_server_version(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.server_version.return_value = "1.2.3"

        runner = CliRunner()
        result = runner.invoke(cli, ["version"])

        assert result.exit_code == 0
        assert "1.2.3" in result.output

    @patch("pymarketstore.cli.pymkts.Client")
    def test_version_with_custom_host_port(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.server_version.return_value = "2.0.0"

        runner = CliRunner()
        result = runner.invoke(cli, ["version", "--host", "myhost", "--port", "9999"])

        MockClient.assert_called_once_with(endpoint="http://myhost:9999/rpc", grpc=False)
        assert result.exit_code == 0

    @patch("pymarketstore.cli.pymkts.Client")
    def test_version_error_shows_message(self, MockClient):
        MockClient.side_effect = Exception("connection refused")

        runner = CliRunner()
        result = runner.invoke(cli, ["version"])

        assert result.exit_code == 1
        assert "Error" in result.output


class TestDeleteCommand:
    @patch("pymarketstore.cli.pymkts.Client")
    def test_delete_no_data_found(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.list_symbols.return_value = []

        runner = CliRunner()
        result = runner.invoke(cli, ["delete", "AAPL", "--yes"])

        assert result.exit_code == 1
        assert "No data found" in result.output

    @patch("pymarketstore.cli.pymkts.Client")
    def test_delete_with_yes_flag(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.list_symbols.return_value = ["AAPL/1D/OHLCV"]
        mock_client.destroy.return_value = {}

        runner = CliRunner()
        result = runner.invoke(cli, ["delete", "AAPL", "--yes"])

        assert result.exit_code == 0
        mock_client.destroy.assert_called_once_with("AAPL/1D/OHLCV")
        assert "Deleted" in result.output

    @patch("pymarketstore.cli.pymkts.Client")
    def test_delete_aborted_by_user(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.list_symbols.return_value = ["AAPL/1D/OHLCV"]

        runner = CliRunner()
        result = runner.invoke(cli, ["delete", "AAPL"], input="n\n")

        assert result.exit_code == 0
        mock_client.destroy.assert_not_called()
        assert "Aborted" in result.output

    @patch("pymarketstore.cli.pymkts.Client")
    def test_delete_with_freq_filter(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.list_symbols.return_value = [
            "AAPL/1D/OHLCV",
            "AAPL/1Min/OHLCV",
        ]
        mock_client.destroy.return_value = {}

        runner = CliRunner()
        result = runner.invoke(cli, ["delete", "AAPL", "--freq", "1Min", "--yes"])

        assert result.exit_code == 0
        mock_client.destroy.assert_called_once_with("AAPL/1Min/OHLCV")


class TestListCommand:
    @patch("pymarketstore.cli.pymkts.Client")
    def test_list_with_tbk_flag(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.list_symbols.return_value = [
            "AAPL/1D/OHLCV",
            "TSLA/1Min/OHLCV",
        ]

        runner = CliRunner()
        result = runner.invoke(cli, ["list", "--tbk"])

        assert result.exit_code == 0
        assert "AAPL/1D/OHLCV" in result.output
        assert "TSLA/1Min/OHLCV" in result.output

    @patch("pymarketstore.cli.pymkts.Client")
    def test_list_with_tbk_and_freq_filter(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.list_symbols.return_value = [
            "AAPL/1D/OHLCV",
            "TSLA/1Min/OHLCV",
        ]

        runner = CliRunner()
        result = runner.invoke(cli, ["list", "--tbk", "--freq", "1D"])

        assert result.exit_code == 0
        assert "AAPL/1D/OHLCV" in result.output
        assert "TSLA/1Min/OHLCV" not in result.output

    @patch("pymarketstore.cli.pymkts.Client")
    def test_list_error_shows_message(self, MockClient):
        MockClient.side_effect = Exception("connection refused")

        runner = CliRunner()
        result = runner.invoke(cli, ["list"])

        assert result.exit_code == 1
        assert "Error" in result.output
