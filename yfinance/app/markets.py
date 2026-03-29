"""Market close time detection from ticker suffix."""

# Mapping: ticker suffix → market close time in UTC (hour, minute)
# Times include a ~1hr buffer after official close for data availability
MARKET_CLOSE_UTC: dict[str, tuple[int, int]] = {
    ".DE": (16, 30),   # XETRA Frankfurt
    ".PA": (16, 30),   # Euronext Paris
    ".AS": (16, 30),   # Euronext Amsterdam
    ".L":  (16, 30),   # London LSE
    ".MC": (16, 30),   # Madrid
    ".MI": (16, 30),   # Milan
    "=F":  (22, 0),    # US Futures (COMEX/NYMEX)
    "=X":  (22, 0),    # Forex (Fri close)
    "-USD": (0, 0),    # Crypto — compact at midnight UTC
}

US_CLOSE_UTC = (21, 0)  # NYSE/NASDAQ default


def get_market_close_utc(symbol: str) -> tuple[int, int]:
    """Return (hour, minute) in UTC when this symbol's market closes.

    Auto-detects from ticker suffix. Falls back to US market hours.
    """
    if symbol.startswith("^"):
        return US_CLOSE_UTC  # US indices
    for suffix, close_time in MARKET_CLOSE_UTC.items():
        if symbol.endswith(suffix):
            return close_time
    return US_CLOSE_UTC  # default: US market
