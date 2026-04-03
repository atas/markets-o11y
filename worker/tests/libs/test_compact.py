from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from db import PriceRow, StaleIntraday
from libs.compact import _fetch_daily_ohlcv, compact_stale_intraday, try_compact


class TestFetchDailyOhlcv:
    @patch("libs.compact.yf.Ticker", side_effect=Exception("network error"))
    def test_yf_exception_returns_none(self, mock_ticker):
        result = _fetch_daily_ohlcv("AAPL", date(2026, 3, 30))
        assert result is None

    @patch("libs.compact.yf.Ticker")
    def test_empty_dataframe_returns_none(self, mock_ticker_cls):
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()
        mock_ticker_cls.return_value = mock_ticker

        result = _fetch_daily_ohlcv("AAPL", date(2026, 3, 30))
        assert result is None

    @patch("libs.compact.yf.Ticker")
    def test_no_matching_date_returns_none(self, mock_ticker_cls):
        idx = pd.DatetimeIndex([datetime(2026, 3, 28, tzinfo=timezone.utc)])
        df = pd.DataFrame(
            {"Open": [100.0], "High": [105.0], "Low": [99.0], "Close": [103.0], "Volume": [1000]},
            index=idx,
        )
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = df
        mock_ticker_cls.return_value = mock_ticker

        result = _fetch_daily_ohlcv("AAPL", date(2026, 3, 30))
        assert result is None

    @patch("libs.compact.yf.Ticker")
    def test_nan_close_returns_none(self, mock_ticker_cls):
        idx = pd.DatetimeIndex([datetime(2026, 3, 30, tzinfo=timezone.utc)])
        df = pd.DataFrame(
            {"Open": [100.0], "High": [105.0], "Low": [99.0], "Close": [np.nan], "Volume": [1000]},
            index=idx,
        )
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = df
        mock_ticker_cls.return_value = mock_ticker

        result = _fetch_daily_ohlcv("AAPL", date(2026, 3, 30))
        assert result is None

    @patch("libs.compact.yf.Ticker")
    def test_matching_row_returns_price_row(self, mock_ticker_cls):
        ts = datetime(2026, 3, 30, tzinfo=timezone.utc)
        idx = pd.DatetimeIndex([ts])
        df = pd.DataFrame(
            {"Open": [100.0], "High": [105.0], "Low": [99.0], "Close": [103.0], "Volume": [1000]},
            index=idx,
        )
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = df
        mock_ticker_cls.return_value = mock_ticker

        result = _fetch_daily_ohlcv("AAPL", date(2026, 3, 30))

        assert isinstance(result, PriceRow)
        assert result.symbol == "AAPL"
        assert result.close == 103.0
        assert result.open == 100.0
        assert result.high == 105.0
        assert result.low == 99.0
        assert result.volume == 1000
        assert result.granularity == "daily"


class TestCompactStaleIntraday:
    @patch("libs.compact.get_stale_intraday_dates", return_value=[])
    def test_no_stale_returns_zero(self, mock_stale):
        conn = MagicMock()
        assert compact_stale_intraday(conn) == 0

    @patch("libs.compact.delete_intraday")
    @patch("libs.compact.insert_prices")
    @patch("libs.compact._fetch_daily_ohlcv", return_value=None)
    @patch("libs.compact.get_stale_intraday_dates")
    def test_daily_not_available_skips(self, mock_stale, mock_fetch, mock_insert, mock_delete):
        mock_stale.return_value = [StaleIntraday("AAPL", date(2026, 3, 29))]
        conn = MagicMock()

        result = compact_stale_intraday(conn)

        assert result == 0
        mock_insert.assert_not_called()
        mock_delete.assert_not_called()

    @patch("libs.compact.delete_intraday", return_value=5)
    @patch("libs.compact.insert_prices")
    @patch("libs.compact._fetch_daily_ohlcv")
    @patch("libs.compact.get_stale_intraday_dates")
    def test_one_entry_compacts(self, mock_stale, mock_fetch, mock_insert, mock_delete):
        mock_stale.return_value = [StaleIntraday("AAPL", date(2026, 3, 29))]
        mock_fetch.return_value = PriceRow(
            time=datetime(2026, 3, 29, tzinfo=timezone.utc),
            symbol="AAPL", open=100.0, high=105.0, low=99.0,
            close=103.0, volume=1000, granularity="daily",
        )
        conn = MagicMock()

        result = compact_stale_intraday(conn)

        assert result == 5
        mock_insert.assert_called_once()
        mock_delete.assert_called_once_with(conn, "AAPL", date(2026, 3, 29))

    @patch("libs.compact.delete_intraday", return_value=3)
    @patch("libs.compact.insert_prices")
    @patch("libs.compact._fetch_daily_ohlcv")
    @patch("libs.compact.get_stale_intraday_dates")
    def test_multiple_entries_mixed(self, mock_stale, mock_fetch, mock_insert, mock_delete):
        mock_stale.return_value = [
            StaleIntraday("AAPL", date(2026, 3, 29)),
            StaleIntraday("MSFT", date(2026, 3, 29)),
        ]
        daily_row = PriceRow(
            time=datetime(2026, 3, 29, tzinfo=timezone.utc),
            symbol="AAPL", open=100.0, high=105.0, low=99.0,
            close=103.0, volume=1000, granularity="daily",
        )
        mock_fetch.side_effect = [daily_row, None]
        conn = MagicMock()

        result = compact_stale_intraday(conn)

        assert result == 3
        assert mock_insert.call_count == 1
        assert mock_delete.call_count == 1


class TestTryCompact:
    @patch("libs.compact.compact_stale_intraday", return_value=3)
    @patch("libs.compact.get_connection")
    def test_calls_compact_and_closes(self, mock_get_conn, mock_compact):
        conn = MagicMock()
        mock_get_conn.return_value = conn

        try_compact()

        mock_compact.assert_called_once_with(conn)
        conn.close.assert_called_once()

    @patch("libs.compact.compact_stale_intraday", side_effect=RuntimeError("db error"))
    @patch("libs.compact.get_connection")
    def test_exception_still_closes(self, mock_get_conn, mock_compact):
        conn = MagicMock()
        mock_get_conn.return_value = conn

        try_compact()

        conn.close.assert_called_once()
