-- Milestone 2: Telegram reaction status indicators + intervention resume
-- (docs/ARCHITECTURE.md Section 21.1). SQLite JSON1 is bundled (sqlx/libsqlite3-sys).

ALTER TABLE listings ADD COLUMN chat_id INTEGER;
ALTER TABLE listings ADD COLUMN anchor_message_id INTEGER;
ALTER TABLE listings ADD COLUMN intervention_prompt_message_id INTEGER;

UPDATE listings SET
  chat_id = COALESCE(chat_id, source_chat_id),
  anchor_message_id = COALESCE(
    anchor_message_id,
    CAST(json_extract(source_message_ids, '$[0]') AS INTEGER)
  )
WHERE chat_id IS NULL OR anchor_message_id IS NULL;

CREATE INDEX idx_listings_chat ON listings(chat_id);
