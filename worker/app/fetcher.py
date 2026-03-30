import logging
from datetime import datetime, timezone
from typing import cast

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


def fetch_history(symbol: str, start: str, end: str, category: str) -> list[tuple]:
    """Fetch historical daily data for a symbol between start and end dates.

    Args:
        symbol: Ticker symbol (e.g. 'AAPL', 'GC=F')
        start: Start date as 'YYYY-MM-DD'
        end: End date as 'YYYY-MM-DD'
        category: Asset category for the DB row

    Returns:
        List of (time, symbol, open, high, low, close, volume, category, granularity) tuples.
    """
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start, end=end, auto_adjust=True)
    except Exception:
        logger.exception("Failed to fetch history for %s", symbol)
        return []

    if df is None or df.empty:
        logger.warning("No historical data returned for %s (%s to %s)", symbol, start, end)
        return []

    rows = []
    for ts, row in df.iterrows():
        ts = cast(pd.Timestamp, ts)
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
            category,
            "daily",
        ))
    return rows


def fetch_current(symbols: list[dict]) -> list[tuple]:
    """Fetch the latest price data for multiple symbols.

    Args:
        symbols: List of dicts with 'symbol' and 'category' keys.

    Returns:
        List of (time, symbol, open, high, low, close, volume, category, granularity) tuples.
    """
    if not symbols:
        return []

    tickers = [s["symbol"] for s in symbols]
    category_map = {s["symbol"]: s["category"] for s in symbols}

    try:
        df = yf.download(tickers, period="1d", interval="15m", group_by="ticker", progress=False)
    except Exception:
        logger.exception("Failed to fetch current prices")
        return []

    if df is None or df.empty:
        logger.warning("No current data returned")
        return []

    rows = []
    now = datetime.now(timezone.utc)

    if len(tickers) == 1:
        sym = tickers[0]
        for ts, row in df.iterrows():
            ts = cast(pd.Timestamp, ts)
            close = _safe_float(row.get("Close"))
            if close is None:
                continue
            rows.append((
                ts.to_pydatetime(),
                sym,
                _safe_float(row.get("Open")),
                _safe_float(row.get("High")),
                _safe_float(row.get("Low")),
                close,
                _safe_int(row.get("Volume")),
                category_map[sym],
                "intraday",
            ))
    else:
        for sym in tickers:
            if sym not in df.columns.get_level_values(0):
                logger.warning("No data for %s in batch download", sym)
                continue
            sym_df = df[sym]
            for ts, row in sym_df.iterrows():
                ts = cast(pd.Timestamp, ts)
                close = _safe_float(row.get("Close"))
                if close is None:
                    continue
                rows.append((
                    ts.to_pydatetime(),
                    sym,
                    _safe_float(row.get("Open")),
                    _safe_float(row.get("High")),
                    _safe_float(row.get("Low")),
                    close,
                    _safe_int(row.get("Volume")),
                    category_map[sym],
                    "intraday",
                ))

    return rows


def _safe_float(val) -> float | None:
    """Convert to float, returning None for NaN."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if f == f else None  # NaN check
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    """Convert to int, returning None for NaN."""
    if val is None:
        return None
    try:
        f = float(val)
        return int(f) if f == f else None
    except (ValueError, TypeError):
        return None
