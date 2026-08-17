-- 002_idempotency.sql: Enforce unique event_id on events table for idempotent ingestion
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id ON events (event_id);
