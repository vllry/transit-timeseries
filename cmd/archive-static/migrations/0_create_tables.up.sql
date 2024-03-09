-- This database structure is based on the following high level schema:
-- * Unique files are upload to an object store.
-- * The database notes every time that a particular file version was "current",
-- as files may be reverted or data may be processed and received out of order.
-- * Uniqueness is identified by the sha256 hash of the file.

-- All known files that get scraped.
CREATE TABLE source_files (
  id SERIAL PRIMARY KEY,
  url TEXT NOT NULL,
    UNIQUE (url),
);

-- All uploaded files.
CREATE TABLE stored_files (
  id SERIAL PRIMARY KEY,
  url TEXT NOT NULL,
  sha256 TEXT NOT NULL,
    UNIQUE (sha256),
);

-- Versons tracks which uploaded files were current at a given time.
-- It provides historical context around when a version must have changed.
CREATE TABLE versions (
  source_id INTEGER NOT NULL REFERENCES source_files(id),
  stored_id INTEGER NOT NULL REFERENCES stored_files(id),
  seen_at TIMESTAMP NOT NULL,
  PRIMARY KEY (source_id, stored_id, seen_at),
);
