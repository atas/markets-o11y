import logging
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = Path("/app/config.yaml")


@dataclass
class SymbolConfig:
    symbol: str
    category: str
    fetch_interval: int  # seconds


@dataclass
class AppConfig:
    fetch_interval: int  # seconds
    history_years: int
    symbols: list[SymbolConfig] = field(default_factory=list)


def _parse_interval(value: str | int) -> int:
    """Parse interval string like '15m', '1h', '30s' to seconds."""
    if isinstance(value, int):
        return value
    value = str(value).strip().lower()
    if value.endswith("h"):
        return int(value[:-1]) * 3600
    if value.endswith("m"):
        return int(value[:-1]) * 60
    if value.endswith("s"):
        return int(value[:-1])
    return int(value)


def load_config(path: Path = CONFIG_PATH) -> AppConfig:
    """Load and validate config.yaml."""
    with open(path) as f:
        raw = yaml.safe_load(f)

    defaults = raw.get("defaults", {})
    global_interval = _parse_interval(defaults.get("fetch_interval", "15m"))
    history_years = int(defaults.get("history_years", 10))

    symbols: list[SymbolConfig] = []
    for category, items in raw.get("symbols", {}).items():
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, str):
                sym = item
                interval = global_interval
            elif isinstance(item, dict):
                sym = item["symbol"]
                interval = _parse_interval(item.get("fetch_interval", global_interval))
            else:
                continue
            symbols.append(SymbolConfig(
                symbol=sym,
                category=category,
                fetch_interval=interval,
            ))

    logger.info("Loaded %d symbols across categories", len(symbols))
    return AppConfig(
        fetch_interval=global_interval,
        history_years=history_years,
        symbols=symbols,
    )
