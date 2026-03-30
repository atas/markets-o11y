# markets-o11y

Self-hosted market observability for stocks, commodities, forex, and crypto. Powered by [yfinance](https://github.com/ranaroussi/yfinance), stored in [TimescaleDB](https://www.timescale.com/), visualized in [Grafana](https://grafana.com/).

```
┌────────────┐     ┌──────────────┐     ┌──────────┐
│   worker   │────>│  TimescaleDB │<────│  Grafana  │
│  (fetcher) │     │  (storage)   │     │  (UI)     │
└────────────┘     └──────────────┘     └──────────┘
     polls              stores            visualizes
  Yahoo Finance      price data          dashboards
  every 15min       (hypertable)         + alerts
```

No accounts. No API keys. No cloud. Just `docker compose up`.

## Quick Start

```bash
git clone https://github.com/YOUR_USERNAME/markets-o11y.git
cd markets-o11y
cp config.example.yaml config.yaml
cp .env.example .env
docker compose up
```

Grafana is at [http://localhost:3000](http://localhost:3000) (default login: admin/admin).

On first run, the fetcher backfills up to 10 years of historical data for all configured symbols, then polls every 15 minutes.

## Configuration

Edit `config.yaml` to customize your watchlist:

```yaml
defaults:
  fetch_interval: 15m
  history_years: 10

symbols:
  stocks:
    - symbol: AAPL
    - symbol: SAP.DE
      fetch_interval: 30m  # per-symbol override
  commodities:
    - symbol: GC=F   # Gold
    - symbol: SI=F   # Silver
  forex:
    - symbol: EURUSD=X
  crypto:
    - symbol: BTC-USD
```

See `config.example.yaml` for the full default watchlist.

## Architecture

| Service | Image | Purpose |
|---------|-------|---------|
| **worker** | Custom Python 3.12 | Fetches prices via yfinance, backfills history, polls on interval |
| **timescaledb** | `timescale/timescaledb:latest-pg16` | Stores all price data in a hypertable |
| **grafana** | `grafana/grafana-oss:latest` | Pre-provisioned dashboards and example alerts |

## Supported Assets

| Category | Examples | Ticker Format |
|----------|----------|---------------|
| US Stocks | AAPL, MSFT, GOOGL | Plain ticker |
| EU Stocks | SAP.DE, MC.PA, ASML.AS | Ticker + exchange suffix |
| Commodities | GC=F (gold), SI=F (silver), CL=F (oil) | Futures with `=F` |
| Forex | EURUSD=X, GBPUSD=X | Pair with `=X` |
| Crypto | BTC-USD, ETH-USD | Pair with `-USD` |
| Indices | ^GSPC (S&P 500), ^DJI (Dow) | `^` prefix |

## Legal Disclaimer

This project is an open-source tool for **personal, non-commercial use only**.

**Regarding data sources:**
- Raw financial price data (stock prices, commodity prices, exchange rates) is factual information and is not subject to copyright under US law (*Feist Publications v. Rural Telephone*, 1991) or EU law.
- However, accessing data via Yahoo Finance or any third-party provider requires agreeing to their respective Terms of Service. **Each user is solely responsible for ensuring their usage complies with the ToS of their chosen data source.**
- This project does not endorse, encourage, or facilitate any violation of third-party Terms of Service.

**General:**
- This software is provided "as is", without warranty of any kind.
- The contributors of this project accept no liability for any financial loss, legal consequences, or data inaccuracies arising from the use of this software.
- **Do not rely on this data for financial decisions.** Always verify prices with an authoritative source.
- This project is not affiliated with Yahoo Finance or any financial exchange.

## License

[AGPL-3.0](LICENSE)
