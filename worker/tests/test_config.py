import pytest

from config import load_config, AppConfig, SymbolConfig


class TestLoadConfig:
    def test_minimal_valid_config(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("""
defaults:
  fetch_interval: 15m
  history_years: 5
symbols:
  stocks:
    - symbol: AAPL
""")
        result = load_config(cfg)

        assert isinstance(result, AppConfig)
        assert result.fetch_interval == 900
        assert result.history_years == 5
        assert len(result.symbols) == 1
        assert result.symbols[0].symbol == "AAPL"
        assert result.symbols[0].fetch_interval == 900

    def test_default_history_years_is_10(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("""
defaults:
  fetch_interval: 15m
symbols:
  stocks:
    - symbol: AAPL
""")
        result = load_config(cfg)
        assert result.history_years == 10

    def test_default_fetch_interval_is_15m(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("""
defaults: {}
symbols:
  stocks:
    - symbol: AAPL
""")
        result = load_config(cfg)
        assert result.fetch_interval == 900

    def test_symbol_as_string_gets_global_interval(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("""
defaults:
  fetch_interval: 30m
symbols:
  stocks:
    - AAPL
""")
        result = load_config(cfg)
        assert result.symbols[0].symbol == "AAPL"
        assert result.symbols[0].fetch_interval == 1800

    def test_symbol_dict_with_override_interval(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("""
defaults:
  fetch_interval: 15m
symbols:
  stocks:
    - symbol: AAPL
      fetch_interval: 5m
""")
        result = load_config(cfg)
        assert result.symbols[0].fetch_interval == 300

    def test_non_list_category_is_skipped(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("""
defaults:
  fetch_interval: 15m
symbols:
  stocks:
    - symbol: AAPL
  broken: not_a_list
""")
        result = load_config(cfg)
        assert len(result.symbols) == 1

    def test_non_str_non_dict_item_is_skipped(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("""
defaults:
  fetch_interval: 15m
symbols:
  stocks:
    - symbol: AAPL
    - 42
    - true
""")
        result = load_config(cfg)
        assert len(result.symbols) == 1

    def test_multiple_categories_flatten(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("""
defaults:
  fetch_interval: 15m
symbols:
  stocks:
    - symbol: AAPL
    - symbol: GOOGL
  crypto:
    - symbol: BTC-USD
""")
        result = load_config(cfg)
        assert len(result.symbols) == 3
        symbols = [s.symbol for s in result.symbols]
        assert "AAPL" in symbols
        assert "GOOGL" in symbols
        assert "BTC-USD" in symbols

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(tmp_path / "nonexistent.yaml")
