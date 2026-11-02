CREATE INDEX IF NOT EXISTS idx_spend_campaign_ts ON spend_events (campaign_id, ts DESC);
