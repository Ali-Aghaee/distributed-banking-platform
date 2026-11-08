-- Metadata + coordinator database
CREATE TABLE shard_map (lo INT PRIMARY KEY, hi INT NOT NULL, shard TEXT NOT NULL, epoch BIGINT NOT NULL);
INSERT INTO shard_map VALUES (1, 1000, 'A', 1), (1001, 2000, 'B', 1);
CREATE TABLE map_epoch (id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1), epoch BIGINT NOT NULL);
INSERT INTO map_epoch VALUES (1, 1);
CREATE TABLE shards (shard TEXT PRIMARY KEY, db_url TEXT NOT NULL, desired_replicas INT NOT NULL, enabled BOOLEAN NOT NULL);
INSERT INTO shards VALUES
  ('A', 'postgresql://bank:bank@db-a:5432/bank', 2, TRUE),
  ('B', 'postgresql://bank:bank@db-b:5432/bank', 2, TRUE),
  ('C', 'postgresql://bank:bank@db-c:5432/bank', 2, FALSE);
CREATE TABLE replicas (
  name TEXT PRIMARY KEY, shard TEXT NOT NULL, url TEXT NOT NULL,
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE tx_log (
  tx_id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL,
  attempt INT NOT NULL DEFAULT 1,
  src TEXT NOT NULL, dst TEXT NOT NULL, amount NUMERIC(18,2) NOT NULL,
  status TEXT NOT NULL,      -- INITIATED PREPARING PREPARED COMMIT_DECIDED ABORT_DECIDED COMPLETED
  outcome TEXT,              -- COMMITTED | ABORTED (only when COMPLETED)
  reason TEXT,
  participants JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (request_id, attempt)
);
CREATE INDEX tx_log_status_idx ON tx_log(status);
CREATE TABLE tx_events (
  id BIGSERIAL PRIMARY KEY, tx_id TEXT NOT NULL, status TEXT NOT NULL,
  detail TEXT, ts TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX tx_events_tx_idx ON tx_events(tx_id);
CREATE TABLE migration (
  id SERIAL PRIMARY KEY,
  src TEXT NOT NULL, dst TEXT NOT NULL, lo INT NOT NULL, hi INT NOT NULL,
  new_epoch BIGINT NOT NULL,
  phase TEXT NOT NULL,       -- PLANNED SNAPSHOT DELTA FENCING ACTIVATING MAP_ACTIVATED CLEANUP DONE FAILED
  snapshot_cursor TEXT NOT NULL DEFAULT '',
  start_xid BIGINT,
  delta_xid BIGINT, delta_seq BIGINT NOT NULL DEFAULT 0,
  info TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
