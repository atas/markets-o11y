"""Compaction: replace previous days' intraday rows with official daily bars."""

import logging
from datetime import date
from typing import cast

import pandas as pd
import yfinance as yf

from utils.convert import safe_float, safe_int
from db import PriceRow, get_connection, get_stale_intraday_dates, insert_prices, delete_intraday

logger = logging.getLogger(__name__)


def _fetch_daily_ohlcv(symbol: str, target_date: date) -> PriceRow | None:
    """Try to fetch the official daily bar for a specific date.

    Returns a PriceRow if available, None if Yahoo hasn't finalized it yet.
    """
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="5d", auto_adjust=True)
    except Exception:
        logger.exception("Failed to fetch daily bar for %s", symbol)
        return None

    if df is None or df.empty:
        return None

    for ts, row in df.iterrows():
        ts = cast(pd.Timestamp, ts)
        if ts.date() == target_date:
            close = safe_float(row.get("Close"))
            if close is None:
                return None
            return PriceRow(
                time=ts.to_pydatetime(),
                symbol=symbol,
                open=safe_float(row.get("Open")),
                high=safe_float(row.get("High")),
                low=safe_float(row.get("Low")),
                close=close,
                volume=safe_int(row.get("Volume")),
                granularity="daily",
            )
    return None


def compact_stale_intraday(conn) -> int:
    """Compact previous days' intraday data into daily bars.

    For each (symbol, date) with intraday rows older than today:
    1. Fetch the official daily bar from Yahoo
    2. Insert the daily bar (dedup handles if already present)
    3. Delete intraday rows for that date
    4. If daily bar not available, skip (retry next cycle)

    Returns total intraday rows deleted.
    """
    stale = get_stale_intraday_dates(conn)
    if not stale:
        return 0

    total_deleted = 0
    for entry in stale:
        daily_ohlcv = _fetch_daily_ohlcv(entry.symbol, entry.date)
        if daily_ohlcv is None:
            logger.debug("Daily bar not yet available for %s on %s", entry.symbol, entry.date)
            continue

        insert_prices(conn, [daily_ohlcv])
        deleted = delete_intraday(conn, entry.symbol, entry.date)
        total_deleted += deleted
        logger.info(
            "Compacted %s %s: deleted %d intraday rows, inserted daily bar",
            entry.symbol, entry.date, deleted,
        )

    return total_deleted


def try_compact():
    """Attempt compaction for stale intraday data. Called after each poll cycle."""
    conn = get_connection()
    try:
        total = compact_stale_intraday(conn)
        if total:
            logger.info("Compaction: cleaned %d stale intraday rows total", total)
    except Exception:
        logger.exception("Compaction error")
    finally:
        conn.close()
