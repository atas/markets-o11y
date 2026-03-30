"""Compaction: replace intraday rows with official daily bars after market close."""

import logging
from datetime import datetime, date, timezone
from typing import cast

import pandas as pd
import yfinance as yf

from config import AppConfig
from utils.convert import safe_float, safe_int
from db import PriceRow, get_connection, has_intraday_rows, insert_prices, delete_intraday, delete_stale_intraday
from config.markets import get_market_close_utc

logger = logging.getLogger(__name__)


def _is_past_close(symbol: str, now: datetime) -> bool:
    """Check if current UTC time is past this symbol's market close."""
    close_h, close_m = get_market_close_utc(symbol)
    return (now.hour, now.minute) > (close_h, close_m)


def _fetch_daily_bar(symbol: str, target_date: date, category: str) -> PriceRow | None:
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
                category=category,
                granularity="daily",
            )
    return None


def try_compact(config: AppConfig):
    """Attempt compaction for all symbols. Called after each poll cycle.

    For each symbol:
    1. Skip if market hasn't closed yet
    2. Skip if no intraday rows exist for today
    3. Try to fetch the official daily bar from Yahoo
    4. If available: insert daily bar, delete intraday rows
    5. If not available: skip, try again next poll
    """
    now = datetime.now(timezone.utc)
    today = now.date()
    conn = get_connection()

    try:
        # Safety net: clean up intraday from previous days
        stale = delete_stale_intraday(conn)
        if stale:
            logger.info("Compaction: cleaned %d stale intraday rows", stale)

        for sym_config in config.symbols:
            symbol = sym_config.symbol

            if not _is_past_close(symbol, now):
                continue

            if not has_intraday_rows(conn, symbol, today):
                continue

            daily_bar = _fetch_daily_bar(symbol, today, sym_config.category)
            if daily_bar is None:
                logger.debug("Daily bar not yet available for %s", symbol)
                continue

            inserted = insert_prices(conn, [daily_bar])
            deleted = delete_intraday(conn, symbol, today)
            logger.info(
                "Compacted %s: deleted %d intraday rows, inserted daily bar",
                symbol, deleted,
            )
    except Exception:
        logger.exception("Compaction error")
    finally:
        conn.close()
