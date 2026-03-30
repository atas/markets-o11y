import pandas as pd

from utils.yf_utils import normalize_download


class TestNormalizeDownload:
    def test_single_ticker(self):
        df = pd.DataFrame({"Close": [100, 101]}, index=pd.date_range("2024-01-01", periods=2))
        result = normalize_download(["AAPL"], df)
        assert list(result.keys()) == ["AAPL"]
        assert result["AAPL"] is df

    def test_multi_ticker(self):
        arrays = [["AAPL", "AAPL", "MSFT", "MSFT"], ["Close", "Volume", "Close", "Volume"]]
        tuples = list(zip(*arrays))
        index = pd.MultiIndex.from_tuples(tuples)
        df = pd.DataFrame(
            [[150, 1000, 300, 2000], [151, 1100, 301, 2100]],
            columns=index,
            index=pd.date_range("2024-01-01", periods=2),
        )
        result = normalize_download(["AAPL", "MSFT"], df)
        assert set(result.keys()) == {"AAPL", "MSFT"}
        assert list(result["AAPL"]["Close"]) == [150, 151]

    def test_missing_ticker_excluded(self):
        arrays = [["AAPL", "AAPL"], ["Close", "Volume"]]
        tuples = list(zip(*arrays))
        index = pd.MultiIndex.from_tuples(tuples)
        df = pd.DataFrame(
            [[150, 1000], [151, 1100]],
            columns=index,
            index=pd.date_range("2024-01-01", periods=2),
        )
        result = normalize_download(["AAPL", "MISSING"], df)
        assert list(result.keys()) == ["AAPL"]
