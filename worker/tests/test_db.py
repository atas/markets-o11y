from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch, call

from db import (
    PriceRow,
    StaleIntraday,
    get_last_timestamp,
    insert_prices,
    delete_intraday,
    has_intraday_rows,
    get_stale_intraday_dates,
)


class TestGetLastTimestamp:
    def test_with_granularity_uses_two_params(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = (datetime(2026, 4, 1, tzinfo=timezone.utc),)

        result = get_last_timestamp(conn, "AAPL", granularity="daily")

        cur.execute.assert_called_once()
        args = cur.execute.call_args
        assert "granularity" in args[0][0]
        assert args[0][1] == ("AAPL", "daily")
        assert result == datetime(2026, 4, 1, tzinfo=timezone.utc)

    def test_without_granularity_uses_one_param(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = (datetime(2026, 4, 1, tzinfo=timezone.utc),)

        result = get_last_timestamp(conn, "AAPL")

        args = cur.execute.call_args
        assert "granularity" not in args[0][0]
        assert args[0][1] == ("AAPL",)
        assert result == datetime(2026, 4, 1, tzinfo=timezone.utc)

    def test_returns_none_when_fetchone_returns_none(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = None

        assert get_last_timestamp(conn, "AAPL") is None

    def test_returns_none_when_fetchone_returns_null_value(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = (None,)

        assert get_last_timestamp(conn, "AAPL") is None


class TestInsertPrices:
    def test_empty_list_returns_zero(self):
        conn = MagicMock()

        result = insert_prices(conn, [])

        assert result == 0
        conn.cursor.assert_not_called()
        conn.commit.assert_not_called()

    @patch("db.execute_values")
    def test_inserts_rows_and_commits(self, mock_exec_values):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.rowcount = 3

        rows = [
            PriceRow(
                time=datetime(2026, 4, 1, tzinfo=timezone.utc),
                symbol="AAPL",
                open=150.0,
                high=155.0,
                low=149.0,
                close=153.0,
                volume=1000000,
                granularity="daily",
            )
        ]

        result = insert_prices(conn, rows)

        mock_exec_values.assert_called_once()
        conn.commit.assert_called_once()
        assert result == 3


class TestDeleteIntraday:
    def test_deletes_and_commits(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.rowcount = 5

        result = delete_intraday(conn, "AAPL", date(2026, 4, 1))

        cur.execute.assert_called_once()
        args = cur.execute.call_args[0]
        assert "DELETE" in args[0]
        assert args[1] == ("AAPL", date(2026, 4, 1), date(2026, 4, 1))
        conn.commit.assert_called_once()
        assert result == 5


class TestHasIntradayRows:
    def test_returns_true_when_row_exists(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = (1,)

        assert has_intraday_rows(conn, "AAPL", date(2026, 4, 1)) is True

    def test_returns_false_when_no_rows(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = None

        assert has_intraday_rows(conn, "AAPL", date(2026, 4, 1)) is False


class TestGetStaleIntradayDates:
    def test_returns_stale_intraday_tuples(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [
            ("AAPL", date(2026, 4, 1)),
            ("GOOGL", date(2026, 4, 1)),
        ]

        result = get_stale_intraday_dates(conn)

        assert len(result) == 2
        assert isinstance(result[0], StaleIntraday)
        assert result[0].symbol == "AAPL"
        assert result[0].date == date(2026, 4, 1)
        assert result[1].symbol == "GOOGL"

    def test_returns_empty_list_when_no_rows(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = []

        result = get_stale_intraday_dates(conn)

        assert result == []
