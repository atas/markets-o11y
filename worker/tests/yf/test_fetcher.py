from datetime import datetime, timedelta, timezone

from db import PriceRow
from yf.fetcher import fetch_date_range, fetch_current


class TestFetchDateRange:
    def test_returns_price_rows_for_valid_ticker(self):
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=7)
        rows = fetch_date_range(
            symbol="AAPL",
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
        )
        assert isinstance(rows, list)
        assert len(rows) > 0
        for row in rows:
            assert isinstance(row, PriceRow)
            assert row.symbol == "AAPL"
            assert row.granularity == "daily"
            assert row.close is not None
            assert isinstance(row.close, float)
            assert isinstance(row.time, datetime)

    def test_nonsense_ticker_returns_empty(self):
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=7)
        rows = fetch_date_range(
            symbol="ZZZXXX999",
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
        )
        assert rows == []


class TestFetchCurrent:
    def test_returns_list(self):
        """fetch_current may return empty on weekends/off-hours, but should always return a list."""
        rows = fetch_current(["AAPL"])
        assert isinstance(rows, list)
        for row in rows:
            assert isinstance(row, PriceRow)
            assert row.symbol == "AAPL"
            assert row.granularity == "intraday"
            assert row.close is not None

    def test_empty_symbols_returns_empty(self):
        assert fetch_current([]) == []
