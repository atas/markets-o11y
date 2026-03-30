import math

import pandas as pd
import pytest

from utils.convert import _scalar, safe_float, safe_int


class TestScalar:
    def test_plain_value(self):
        assert _scalar(42) == 42

    def test_none(self):
        assert _scalar(None) is None

    def test_single_element_series(self):
        s = pd.Series([3.14])
        assert _scalar(s) == 3.14

    def test_multi_element_series_returns_none(self):
        s = pd.Series([1.0, 2.0, 3.0])
        assert _scalar(s) is None

    def test_empty_series_returns_none(self):
        s = pd.Series([], dtype=float)
        assert _scalar(s) is None


class TestSafeFloat:
    def test_int(self):
        assert safe_float(42) == 42.0

    def test_float(self):
        assert safe_float(3.14) == 3.14

    def test_string_number(self):
        assert safe_float("99.5") == 99.5

    def test_none(self):
        assert safe_float(None) is None

    def test_nan(self):
        assert safe_float(float("nan")) is None

    def test_pandas_nat(self):
        assert safe_float(pd.NaT) is None

    def test_non_numeric_string(self):
        assert safe_float("abc") is None

    def test_series_single(self):
        assert safe_float(pd.Series([5.5])) == 5.5

    def test_series_multi(self):
        assert safe_float(pd.Series([1.0, 2.0])) is None


class TestSafeInt:
    def test_int(self):
        assert safe_int(42) == 42

    def test_float_truncates(self):
        assert safe_int(3.9) == 3

    def test_string_number(self):
        assert safe_int("100") == 100

    def test_none(self):
        assert safe_int(None) is None

    def test_nan(self):
        assert safe_int(float("nan")) is None

    def test_non_numeric_string(self):
        assert safe_int("abc") is None

    def test_series_single(self):
        assert safe_int(pd.Series([7])) == 7
