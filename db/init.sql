CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS prices (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      BIGINT,
    category    TEXT,
    granularity TEXT NOT NULL DEFAULT 'daily'
);

SELECT create_hypertable('prices', 'time', if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS idx_prices_symbol_time_gran
    ON prices (symbol, time, granularity);

CREATE INDEX IF NOT EXISTS idx_prices_category
    ON prices (category, time DESC);
