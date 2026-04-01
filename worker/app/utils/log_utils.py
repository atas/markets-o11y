import logging


class YFinanceDelistFilter(logging.Filter):
    """Downgrade yfinance 'possibly delisted' errors to warnings."""
    def filter(self, record):
        if record.levelno == logging.ERROR and "possibly delisted; no price data found" in record.getMessage():
            record.levelno = logging.WARNING
            record.levelname = "WARNING"
        return True
