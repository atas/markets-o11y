from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from config import SymbolConfig
from db import PriceRow
from libs.backfill import _backfill_daily, _backfill_intraday


class TestBackfillDaily:
    @patch("libs.backfill.insert_prices")
    @patch("libs.backfill.get_last_timestamp", return_value=None)
    def test_fresh_backfill_fetches_and_inserts(self, mock_last_ts, mock_insert):
        mock_insert.return_value = 5
        conn = MagicMock()
        sym = SymbolConfig(symbol="AAPL", fetch_interval=900)

        result = _backfill_daily(conn, sym, history_years=1)

        mock_last_ts.assert_called_once_with(conn, "AAPL", granularity="daily")
        mock_insert.assert_called_once()
        rows = mock_insert.call_args[0][1]
        assert isinstance(rows, list)
        assert len(rows) > 0
        for row in rows:
            assert isinstance(row, PriceRow)
            assert row.symbol == "AAPL"
            assert row.granularity == "daily"
        assert result == 5

    @patch("libs.backfill.insert_prices")
    @patch("libs.backfill.get_last_timestamp")
    def test_recent_data_skips_fetch(self, mock_last_ts, mock_insert):
        mock_last_ts.return_value = datetime.now(timezone.utc) - timedelta(hours=12)
        conn = MagicMock()
        sym = SymbolConfig(symbol="AAPL", fetch_interval=900)

        result = _backfill_daily(conn, sym, history_years=1)

        assert result == 0
        mock_insert.assert_not_called()


class TestBackfillIntraday:
    @patch("libs.backfill.insert_prices")
    def test_intraday_no_crash(self, mock_insert):
        """Intraday may return empty on weekends — just verify no crash."""
        mock_insert.return_value = 0
        conn = MagicMock()
        sym = SymbolConfig(symbol="AAPL", fetch_interval=900)

        result = _backfill_intraday(conn, sym)

        assert isinstance(result, int)
        assert result >= 0
