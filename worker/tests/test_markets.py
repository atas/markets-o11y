from config.markets import get_market_close_utc


class TestGetMarketCloseUtc:
    def test_us_stock(self):
        assert get_market_close_utc("AAPL") == (21, 0)

    def test_us_index(self):
        assert get_market_close_utc("^GSPC") == (21, 0)

    def test_german_stock(self):
        assert get_market_close_utc("SAP.DE") == (16, 30)

    def test_london_stock(self):
        assert get_market_close_utc("VWRD.L") == (16, 30)

    def test_futures(self):
        assert get_market_close_utc("GC=F") == (22, 0)

    def test_forex(self):
        assert get_market_close_utc("EURUSD=X") == (22, 0)

    def test_crypto(self):
        assert get_market_close_utc("BTC-USD") == (0, 0)

    def test_unknown_defaults_to_us(self):
        assert get_market_close_utc("UNKNOWN") == (21, 0)
