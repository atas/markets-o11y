import logging
import time

import schedule

from yf.backfill import backfill_all
from compact import try_compact
from config import AppConfig, load_config
from db import get_connection, insert_prices
from yf.fetcher import fetch_current

class _YFinanceDelistFilter(logging.Filter):
    """Downgrade yfinance 'possibly delisted' errors to warnings."""
    def filter(self, record):
        if record.levelno == logging.ERROR and "possibly delisted; no price data found" in record.getMessage():
            record.levelno = logging.WARNING
            record.levelname = "WARNING"
        return True


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger("yfinance").addFilter(_YFinanceDelistFilter())
logger = logging.getLogger(__name__)


def poll(config: AppConfig):
    """Fetch current prices for all configured symbols and insert into DB."""
    symbols = [{"symbol": s.symbol, "category": s.category} for s in config.symbols]
    logger.info("Polling %d symbols", len(symbols))

    rows = fetch_current(symbols)
    if not rows:
        logger.warning("No data returned from poll")
        return

    conn = get_connection()
    try:
        inserted = insert_prices(conn, rows)
        logger.info("Poll complete: %d rows inserted", inserted)
    finally:
        conn.close()

    # Try to compact intraday data for symbols whose market has closed
    try_compact(config)


def main():
    logger.info("Starting markets-o11y worker")

    config = load_config()
    logger.info(
        "Config: %d symbols, fetch_interval=%ds, history_years=%d",
        len(config.symbols),
        config.fetch_interval,
        config.history_years,
    )

    # Backfill historical data and fill any gaps
    backfill_all(config)

    # Initial poll right after backfill
    poll(config)

    # Schedule recurring polls
    interval_minutes = max(1, config.fetch_interval // 60)
    schedule.every(interval_minutes).minutes.do(poll, config=config)
    logger.info("Scheduled polling every %d minutes", interval_minutes)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
