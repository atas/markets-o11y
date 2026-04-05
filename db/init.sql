CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS prices (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      BIGINT,
    granularity TEXT NOT NULL DEFAULT 'daily'
);

SELECT create_hypertable('prices', 'time', if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS idx_prices_symbol_time_gran
    ON prices (symbol, time, granularity);

CREATE OR REPLACE FUNCTION get_symbol_prices(
    p_symbol TEXT,
    p_interval INTERVAL,
    p_from TIMESTAMPTZ,
    p_to TIMESTAMPTZ
) RETURNS TABLE("time" TIMESTAMPTZ, value DOUBLE PRECISION) AS $$
    SELECT time_bucket(p_interval, t.time) AS time, avg(t.close) AS value
    FROM prices t
    WHERE t.time BETWEEN p_from AND p_to
      AND t.symbol = p_symbol
      AND NOT (t.granularity = 'intraday' AND t.time < CURRENT_DATE)
    GROUP BY 1
    ORDER BY 1;
$$ LANGUAGE sql STABLE;

