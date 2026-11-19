CREATE TABLE IF NOT EXISTS telegram_cursor (
  id         INTEGER PRIMARY KEY CHECK (id = 1),
  update_id  INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS listings (
  id                 TEXT PRIMARY KEY,                       -- ULID
  media_group_id     TEXT,
  sender_id          INTEGER NOT NULL,
  source_chat_id     INTEGER NOT NULL,
  source_message_ids TEXT NOT NULL DEFAULT '[]',             -- JSON array of TG message ids
  status             TEXT NOT NULL DEFAULT 'INGESTED'
                     CHECK (status IN ('INGESTED','ENRICHED','MEDIA_READY','PUBLISHED','NEEDS_INTERVENTION')),
  title              TEXT,
  price_rm           REAL,
  category           TEXT,
  location           TEXT,
  tenure             TEXT,
  built_up_area      TEXT,
  land_area          TEXT,
  furnishing         TEXT,
  ad_copy            TEXT,
  layout_variation   INTEGER,
  cover_asset_id     TEXT,
  watermark_text     TEXT,
  intervention_reason TEXT,
  error_count        INTEGER NOT NULL DEFAULT 0,
  last_error         TEXT,
  ingested_at        INTEGER NOT NULL,
  enriched_at        INTEGER,
  media_ready_at     INTEGER,
  published_at       INTEGER,
  updated_at         INTEGER NOT NULL,
  fb_post_id         TEXT
);
CREATE INDEX idx_listings_status      ON listings(status, ingested_at);
CREATE INDEX idx_listings_sender      ON listings(sender_id);
CREATE UNIQUE INDEX uq_listings_group ON listings(media_group_id, sender_id)
  WHERE media_group_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS media_assets (
  id               TEXT PRIMARY KEY,                         -- ULID
  listing_id       TEXT NOT NULL REFERENCES listings(id) ON DELETE CASCADE,
  file_unique_id   TEXT NOT NULL,
  file_id          TEXT,
  media_type       TEXT NOT NULL CHECK (media_type IN ('photo','video')),
  local_path       TEXT,
  sort_order       INTEGER NOT NULL DEFAULT 0,
  width            INTEGER,
  height           INTEGER,
  laplacian_score  REAL,
  dhash            TEXT,
  passed_stage1    INTEGER NOT NULL DEFAULT 0,
  is_qr            INTEGER NOT NULL DEFAULT 0,
  is_flyer         INTEGER NOT NULL DEFAULT 0,
  is_cover         INTEGER NOT NULL DEFAULT 0,
  stage2_rank      INTEGER,
  perturbed        INTEGER NOT NULL DEFAULT 0,
  brightness_delta REAL,
  crop_delta       TEXT,
  exif_cleaned     INTEGER NOT NULL DEFAULT 0,
  status           TEXT NOT NULL DEFAULT 'DOWNLOADED'
                   CHECK (status IN ('DOWNLOADED','FILTERED_OUT','WATERMARKED','READY')),
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL
);
CREATE UNIQUE INDEX uq_media_unique  ON media_assets(file_unique_id);
CREATE INDEX idx_media_listing       ON media_assets(listing_id, sort_order);

CREATE TABLE IF NOT EXISTS publish_attempts (
  id          TEXT PRIMARY KEY,                              -- ULID
  listing_id  TEXT NOT NULL REFERENCES listings(id),
  started_at  INTEGER NOT NULL,
  finished_at INTEGER,
  outcome     TEXT CHECK (outcome IN ('SUCCESS','FAILED','ROLLED_BACK','PAUSED')),
  stage       TEXT,                                          -- last CDP stage reached
  error       TEXT,
  fb_post_id  TEXT
);
CREATE INDEX idx_publish_started ON publish_attempts(started_at);   -- rolling-window rate limit
CREATE INDEX idx_publish_listing ON publish_attempts(listing_id);

CREATE TABLE IF NOT EXISTS supervisor_events (
  id          TEXT PRIMARY KEY,
  listing_id  TEXT,
  event_type  TEXT NOT NULL,
  severity    TEXT NOT NULL CHECK (severity IN ('INFO','WARN','CRITICAL')),
  detail      TEXT,
  resolved    INTEGER NOT NULL DEFAULT 0,
  created_at  INTEGER NOT NULL,
  resolved_at INTEGER
);
CREATE INDEX idx_supervisor_created ON supervisor_events(created_at);

CREATE TABLE IF NOT EXISTS daemon_state (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
