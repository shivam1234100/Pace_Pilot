-- PacePilot schema. Money is integer cents everywhere, never floats.
-- File is prefixed 001_ so docker-entrypoint-initdb.d runs it before 002_indexes.sql
-- (initdb runs files in alphabetical order; "schema.sql" would sort after "002_...").
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE campaigns (
  id                 BIGSERIAL PRIMARY KEY,
  name               TEXT NOT NULL,
  platform           TEXT NOT NULL CHECK (platform IN ('meta','google','tiktok')),
  external_id        TEXT NOT NULL,
  daily_budget_cents BIGINT NOT NULL CHECK (daily_budget_cents > 0),
  status             TEXT NOT NULL DEFAULT 'active'
                     CHECK (status IN ('active','paused','overpacing')),
  breach_at          TIMESTAMPTZ,
  paused_at          TIMESTAMPTZ,
  pause_lag_ms       INT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ledger of approved spend. No FK to campaigns: keeps ingest fast (documented trade-off).
CREATE TABLE spend_events (
  ts           TIMESTAMPTZ NOT NULL,
  campaign_id  BIGINT      NOT NULL,
  event_id     TEXT        NOT NULL,
  amount_cents BIGINT      NOT NULL CHECK (amount_cents > 0),
  UNIQUE (event_id, ts)   -- unique constraints on hypertables must include the time column
);

SELECT create_hypertable('spend_events', 'ts', chunk_time_interval => INTERVAL '1 day');

-- NOTE: deliberately NO (campaign_id, ts) index here. The benchmark adds it
-- and measures the difference. It lives in 002_indexes.sql and is kept.

CREATE MATERIALIZED VIEW spend_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT campaign_id,
       time_bucket('1 hour', ts) AS bucket,
       sum(amount_cents)         AS spend_cents,
       count(*)                  AS events
FROM spend_events
GROUP BY campaign_id, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('spend_hourly',
  start_offset      => INTERVAL '3 days',
  end_offset        => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute');
