import pytest

from utils.time import parse_interval


class TestParseInterval:
    def test_minutes(self):
        assert parse_interval("15m") == 900

    def test_hours(self):
        assert parse_interval("1h") == 3600

    def test_seconds(self):
        assert parse_interval("30s") == 30

    def test_plain_int_string(self):
        assert parse_interval("900") == 900

    def test_int_passthrough(self):
        assert parse_interval(900) == 900

    def test_uppercase_stripped(self):
        assert parse_interval("  2H  ") == 7200

    def test_multi_digit_minutes(self):
        assert parse_interval("30m") == 1800
