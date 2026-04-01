import logging
from datetime import datetime, timedelta, timezone
from typing import cast

import pandas as pd
import yfinance as yf

from config import AppConfig, SymbolConfig
from utils.convert import safe_float, safe_int
from db import PriceRow, get_connection, get_last_timestamp, insert_prices
from yf.fetcher import fetch_date_range

logger = logging.getLogger(__name__)


def _backfill_daily(conn, symbol_config: SymbolConfig, history_years: int) -> int:
    """Backfill daily historical data for a symbol. Returns rows inserted."""
    symbol = symbol_config.symbol
    last_daily = get_last_timestamp(conn, symbol, granularity="daily")
    now = datetime.now(timezone.utc)

    if last_daily is None:
        start = now - timedelta(days=history_years * 365)
        logger.info("No daily data for %s — backfilling %d years", symbol, history_years)
    else:
        gap_days = (now - last_daily).days
        if gap_days <= 1:
            return 0
        start = last_daily + timedelta(days=1)
        logger.info("Filling %d-day daily gap for %s", gap_days, symbol)

    rows = fetch_date_range(
        symbol=symbol,
        start=start.strftime("%Y-%m-%d"),
        end=now.strftime("%Y-%m-%d"),
        category=symbol_config.category,
    )

    if rows:
        inserted = insert_prices(conn, rows)
        logger.info("Daily backfill: %d rows for %s", inserted, symbol)
        return inserted
    return 0


def _backfill_intraday(conn, symbol_config: SymbolConfig) -> int:
    """Fetch today's intraday data for a symbol. Returns rows inserted.

    Always refetches the full day — ON CONFLICT DO NOTHING handles dedup.
    """
    symbol = symbol_config.symbol

    try:
        df = yf.download(symbol, period="1d", interval="15m", progress=False)
    except Exception:
        logger.exception("Failed to fetch intraday for %s", symbol)
        return 0

    if df is None or df.empty:
        return 0

    rows: list[PriceRow] = []
    for ts, row in df.iterrows():
        ts = cast(pd.Timestamp, ts)
        close = safe_float(row.get("Close"))
        if close is None:
            continue
        rows.append(PriceRow(
            time=ts.to_pydatetime(),
            symbol=symbol,
            open=safe_float(row.get("Open")),
            high=safe_float(row.get("High")),
            low=safe_float(row.get("Low")),
            close=close,
            volume=safe_int(row.get("Volume")),
            category=symbol_config.category,
            granularity="intraday",
        ))

    if rows:
        inserted = insert_prices(conn, rows)
        logger.info("Intraday backfill: %d rows for %s", inserted, symbol)
        return inserted
    return 0


def backfill_all(config: AppConfig) -> int:
    """Backfill all configured symbols: daily history + today's intraday.

    Returns total rows inserted.
    """
    conn = get_connection()
    total = 0
    try:
        for sym_config in config.symbols:
            try:
                total += _backfill_daily(conn, sym_config, config.history_years)
                total += _backfill_intraday(conn, sym_config)
            except Exception:
                logger.exception("Failed to backfill %s", sym_config.symbol)
    finally:
        conn.close()

    logger.info("Backfill complete: %d total rows inserted", total)
    return total
