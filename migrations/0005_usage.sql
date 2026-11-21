-- Milestone 9: daily usage telemetry (Section 29.1).
ALTER TABLE listings ADD COLUMN prompt_tokens INTEGER;
ALTER TABLE listings ADD COLUMN completion_tokens INTEGER;

CREATE TABLE IF NOT EXISTS daily_usage (
  date              TEXT PRIMARY KEY,
  listings_count    INTEGER NOT NULL DEFAULT 0,
  images_processed  INTEGER NOT NULL DEFAULT 0,
  prompt_tokens     INTEGER NOT NULL DEFAULT 0,
  completion_tokens INTEGER NOT NULL DEFAULT 0
);
