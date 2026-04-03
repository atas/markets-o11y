import logging
import os
from datetime import date, datetime
from typing import NamedTuple

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


class PriceRow(NamedTuple):
    time: datetime
    symbol: str
    open: float | None
    high: float | None
    low: float | None
    close: float
    volume: int | None
    granularity: str


class StaleIntraday(NamedTuple):
    symbol: str
    date: date


def get_connection():
    """Create a new database connection from environment variables."""
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "timescaledb"),
        port=int(os.environ.get("DB_PORT", 5432)),
        user=os.environ.get("DB_USER", "markets"),
        password=os.environ.get("DB_PASSWORD", "markets"),
        dbname=os.environ.get("DB_NAME", "markets"),
    )


def get_last_timestamp(conn, symbol: str, granularity: str | None = None) -> datetime | None:
    """Get the most recent timestamp for a symbol, or None if no data."""
    if granularity:
        query = "SELECT MAX(time) FROM prices WHERE symbol = %s AND granularity = %s"
        params = (symbol, granularity)
    else:
        query = "SELECT MAX(time) FROM prices WHERE symbol = %s"
        params = (symbol,)
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def insert_prices(conn, rows: list[PriceRow]) -> int:
    """Batch insert price rows. Returns number of rows inserted.

    Duplicates are silently skipped via ON CONFLICT.
    """
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO prices (time, symbol, open, high, low, close, volume, granularity)
            VALUES %s
            ON CONFLICT (symbol, time, granularity) DO NOTHING
            """,
            rows,
            page_size=1000,
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def delete_intraday(conn, symbol: str, date: date | datetime) -> int:
    """Delete intraday rows for a symbol on a specific date. Returns rows deleted."""
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM prices
            WHERE symbol = %s
              AND granularity = 'intraday'
              AND time >= %s
              AND time < %s + INTERVAL '1 day'
            """,
            (symbol, date, date),
        )
        deleted = cur.rowcount
    conn.commit()
    return deleted


def has_intraday_rows(conn, symbol: str, date: date | datetime) -> bool:
    """Check if intraday rows exist for a symbol on a given date."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM prices
            WHERE symbol = %s
              AND granularity = 'intraday'
              AND time >= %s
              AND time < %s + INTERVAL '1 day'
            LIMIT 1
            """,
            (symbol, date, date),
        )
        return cur.fetchone() is not None


def get_stale_intraday_dates(conn) -> list[StaleIntraday]:
    """Return previous days' intraday (symbol, date) entries,
    but only for symbols that already have new intraday data today."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT p.symbol, p.time::date AS d
            FROM prices p
            WHERE p.granularity = 'intraday'
              AND p.time < CURRENT_DATE
              AND EXISTS (
                  SELECT 1 FROM prices t
                  WHERE t.symbol = p.symbol
                    AND t.granularity = 'intraday'
                    AND t.time >= CURRENT_DATE
              )
            ORDER BY p.symbol, d
            """
        )
        return [StaleIntraday(*row) for row in cur.fetchall()]
