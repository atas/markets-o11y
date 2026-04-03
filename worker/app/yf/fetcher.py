import logging
from typing import cast

import pandas as pd
import yfinance as yf

from utils.convert import safe_float, safe_int
from db import PriceRow
from utils.yf_utils import normalize_download

logger = logging.getLogger(__name__)


def fetch_date_range(symbol: str, start: str, end: str) -> list[PriceRow]:
    """Fetch historical daily data for a symbol between start and end dates."""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start, end=end, auto_adjust=True)
    except Exception:
        logger.exception("Failed to fetch history for %s", symbol)
        return []

    if df is None or df.empty:
        logger.warning("No historical data returned for %s (%s to %s)", symbol, start, end)
        return []

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
            granularity="daily",
        ))
    return rows


def fetch_current(symbols: list[str]) -> list[PriceRow]:
    """Fetch the latest 15-minute bar for all symbols in a single batch call.

    Fetches full day's data — dedup via ON CONFLICT handles overlap.
    Also self-heals gaps from laptop sleep or downtime.
    """
    if not symbols:
        return []

    tickers = symbols

    try:
        df = yf.download(tickers, period="1d", interval="15m", group_by="ticker", progress=False)
    except Exception:
        logger.exception("Failed to fetch current prices")
        return []

    if df is None or df.empty:
        logger.warning("No current data returned")
        return []

    ticker_dfs = normalize_download(tickers, df)

    rows: list[PriceRow] = []
    for sym, sym_df in ticker_dfs.items():
        sym_df = sym_df.dropna(how="all")
        if sym_df.empty:
            continue
        last = sym_df.iloc[-1]
        close = safe_float(last.get("Close"))
        if close is None:
            continue
        rows.append(PriceRow(
            time=cast(pd.Timestamp, sym_df.index[-1]).to_pydatetime(),
            symbol=sym,
            open=safe_float(last.get("Open")),
            high=safe_float(last.get("High")),
            low=safe_float(last.get("Low")),
            close=close,
            volume=safe_int(last.get("Volume")),
            granularity="intraday",
        ))

    return rows

