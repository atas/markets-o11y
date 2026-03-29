import logging
from datetime import datetime, timedelta, timezone

import yfinance as yf

from config import AppConfig, SymbolConfig
from db import get_connection, get_last_timestamp, insert_prices, delete_stale_intraday, cleanup_duplicate_daily
from fetcher import fetch_history, _safe_float, _safe_int

logger = logging.getLogger(__name__)


def backfill_daily(conn, symbol_config: SymbolConfig, history_years: int) -> int:
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

    rows = fetch_history(
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


def backfill_intraday(conn, symbol_config: SymbolConfig) -> int:
    """Fetch today's intraday data for a symbol. Returns rows inserted.

    Always refetches the full day — ON CONFLICT DO NOTHING handles dedup.
    """
    symbol = symbol_config.symbol

    try:
        df = yf.download(symbol, period="1d", interval="15m", progress=False)
    except Exception:
        logger.exception("Failed to fetch intraday for %s", symbol)
        return 0

    if df.empty:
        return 0

    rows = []
    for ts, row in df.iterrows():
        close = _safe_float(row.get("Close"))
        if close is None:
            continue
        rows.append((
            ts.to_pydatetime(),
            symbol,
            _safe_float(row.get("Open")),
            _safe_float(row.get("High")),
            _safe_float(row.get("Low")),
            close,
            _safe_int(row.get("Volume")),
            symbol_config.category,
            "intraday",
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
        # Clean up any stale intraday rows from previous days
        stale = delete_stale_intraday(conn)
        if stale:
            logger.info("Cleaned up %d stale intraday rows", stale)

        # Remove duplicate daily rows (e.g. mis-tagged intraday from before migration)
        dupes = cleanup_duplicate_daily(conn)
        if dupes:
            logger.info("Cleaned up %d duplicate daily rows", dupes)

        for sym_config in config.symbols:
            try:
                total += backfill_daily(conn, sym_config, config.history_years)
                total += backfill_intraday(conn, sym_config)
            except Exception:
                logger.exception("Failed to backfill %s", sym_config.symbol)
    finally:
        conn.close()

    logger.info("Backfill complete: %d total rows inserted", total)
    return total
