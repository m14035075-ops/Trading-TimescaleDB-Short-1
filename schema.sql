-- =============================================================
-- TimescaleDB Schema for OpenAlgo Tick + Level-2 Depth Recorder
-- =============================================================
-- एक बार चलाएँ:  psql -U postgres -d marketdata -f schema.sql
-- =============================================================

-- TimescaleDB extension (एक बार)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- -------------------------------------------------------------
-- 1) ticks  → हर 1 second का OHLCV + LTP snapshot (mode 2 Quote)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ticks (
    time              TIMESTAMPTZ      NOT NULL,
    exchange          TEXT             NOT NULL,
    symbol            TEXT             NOT NULL,
    ltp               DOUBLE PRECISION,
    volume            BIGINT,
    open              DOUBLE PRECISION,
    high              DOUBLE PRECISION,
    low               DOUBLE PRECISION,
    close             DOUBLE PRECISION,
    last_trade_qty    BIGINT,
    avg_trade_price   DOUBLE PRECISION,
    change            DOUBLE PRECISION,
    change_percent    DOUBLE PRECISION
);

-- Hypertable (time partition: 1 दिन का chunk)
SELECT create_hypertable(
    'ticks', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- तेज़ lookup के लिए index
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_time
    ON ticks (exchange, symbol, time DESC);

-- -------------------------------------------------------------
-- 2) depth  → Level-2 order book (mode 3 Market Depth, 5 levels)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS depth (
    time         TIMESTAMPTZ      NOT NULL,
    exchange     TEXT             NOT NULL,
    symbol       TEXT             NOT NULL,
    ltp          DOUBLE PRECISION,
    side         TEXT             NOT NULL,   -- 'buy' / 'sell'
    level        SMALLINT         NOT NULL,   -- 1..5
    price        DOUBLE PRECISION,
    quantity     BIGINT,
    orders       INTEGER
);

SELECT create_hypertable(
    'depth', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_depth_symbol_time
    ON depth (exchange, symbol, time DESC);

-- -------------------------------------------------------------
-- 2b) bars_1m  → backfill के लिए 1-minute OHLCV (REST history API से)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bars_1m (
    time      TIMESTAMPTZ      NOT NULL,
    exchange  TEXT             NOT NULL,
    symbol    TEXT             NOT NULL,
    open      DOUBLE PRECISION,
    high      DOUBLE PRECISION,
    low       DOUBLE PRECISION,
    close     DOUBLE PRECISION,
    volume    BIGINT,
    PRIMARY KEY (exchange, symbol, time)         -- duplicate insert safe
);

SELECT create_hypertable(
    'bars_1m', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists       => TRUE
);

-- -------------------------------------------------------------
-- 3) Compression  → 7 दिन पुराने chunks compress होंगे (~10x कम जगह)
-- -------------------------------------------------------------
ALTER TABLE ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, symbol',
    timescaledb.compress_orderby   = 'time DESC'
);
SELECT add_compression_policy('ticks', INTERVAL '7 days', if_not_exists => TRUE);

ALTER TABLE depth SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, symbol, side',
    timescaledb.compress_orderby   = 'time DESC'
);
SELECT add_compression_policy('depth', INTERVAL '7 days', if_not_exists => TRUE);

ALTER TABLE bars_1m SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, symbol',
    timescaledb.compress_orderby   = 'time DESC'
);
SELECT add_compression_policy('bars_1m', INTERVAL '30 days', if_not_exists => TRUE);

-- -------------------------------------------------------------
-- 4) Retention (optional) — 180 दिन के बाद auto-delete
--    ज़रूरत हो तो uncomment कर दें
-- -------------------------------------------------------------
-- SELECT add_retention_policy('ticks', INTERVAL '180 days', if_not_exists => TRUE);
-- SELECT add_retention_policy('depth', INTERVAL '180 days', if_not_exists => TRUE);

-- -------------------------------------------------------------
-- 5) उपयोगी continuous aggregate — 1-minute OHLCV (optional)
-- -------------------------------------------------------------
-- CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1m
-- WITH (timescaledb.continuous) AS
-- SELECT
--     time_bucket('1 minute', time) AS bucket,
--     exchange, symbol,
--     first(ltp, time) AS open,
--     max(ltp)         AS high,
--     min(ltp)         AS low,
--     last(ltp, time)  AS close,
--     max(volume)      AS volume
-- FROM ticks
-- GROUP BY bucket, exchange, symbol;
--
-- SELECT add_continuous_aggregate_policy('ohlcv_1m',
--   start_offset => INTERVAL '1 day',
--   end_offset   => INTERVAL '1 minute',
--   schedule_interval => INTERVAL '1 minute');
