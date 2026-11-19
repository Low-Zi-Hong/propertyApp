-- Milestone 4: publisher pre-flight fields + DRY_RUN publish outcome.
ALTER TABLE listings ADD COLUMN bedrooms INTEGER;
ALTER TABLE listings ADD COLUMN bathrooms INTEGER;
ALTER TABLE listings ADD COLUMN property_type TEXT;

CREATE TABLE publish_attempts_new (
  id          TEXT PRIMARY KEY,
  listing_id  TEXT NOT NULL REFERENCES listings(id),
  started_at  INTEGER NOT NULL,
  finished_at INTEGER,
  outcome     TEXT CHECK (outcome IN ('SUCCESS','FAILED','ROLLED_BACK','PAUSED','DRY_RUN')),
  stage       TEXT,
  error       TEXT,
  fb_post_id  TEXT
);
INSERT INTO publish_attempts_new (id, listing_id, started_at, finished_at, outcome, stage, error, fb_post_id)
  SELECT id, listing_id, started_at, finished_at, outcome, stage, error, fb_post_id FROM publish_attempts;
DROP TABLE publish_attempts;
ALTER TABLE publish_attempts_new RENAME TO publish_attempts;
CREATE INDEX idx_publish_started ON publish_attempts(started_at);
CREATE INDEX idx_publish_listing ON publish_attempts(listing_id);
