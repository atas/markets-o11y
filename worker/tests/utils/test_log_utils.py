import logging

from utils.log_utils import YFinanceDelistFilter


class TestYFinanceDelistFilter:
    def _make_record(self, level, msg):
        record = logging.LogRecord(
            name="yfinance",
            level=level,
            pathname="",
            lineno=0,
            msg=msg,
            args=(),
            exc_info=None,
        )
        return record

    def test_error_with_delisted_msg_downgraded_to_warning(self):
        f = YFinanceDelistFilter()
        record = self._make_record(logging.ERROR, "AAPL: possibly delisted; no price data found")

        result = f.filter(record)

        assert result is True
        assert record.levelno == logging.WARNING
        assert record.levelname == "WARNING"

    def test_error_with_other_msg_unchanged(self):
        f = YFinanceDelistFilter()
        record = self._make_record(logging.ERROR, "some other error")

        result = f.filter(record)

        assert result is True
        assert record.levelno == logging.ERROR
        assert record.levelname == "ERROR"

    def test_warning_record_unchanged(self):
        f = YFinanceDelistFilter()
        record = self._make_record(logging.WARNING, "possibly delisted")

        result = f.filter(record)

        assert result is True
        assert record.levelno == logging.WARNING
        assert record.levelname == "WARNING"
