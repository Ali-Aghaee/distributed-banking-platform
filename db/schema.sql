-- Shared schema of every shard database.
CREATE TABLE accounts (
  id      TEXT PRIMARY KEY,
  balance NUMERIC(18,2) NOT NULL CHECK (balance >= 0),
  version BIGINT NOT NULL DEFAULT 0
);
-- Ownership / fencing table: one row per chunk of 100 accounts.
-- status: ACTIVE | FENCED | MOVED | INCOMING ; epoch = shard-map epoch of the grant.
CREATE TABLE chunks (
  chunk_id INT PRIMARY KEY,
  lo INT NOT NULL, hi INT NOT NULL,
  status TEXT NOT NULL, epoch BIGINT NOT NULL
);
-- Operation journal: idempotency (exactly-once) + audit trail.
CREATE TABLE journal (
  request_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  op TEXT NOT NULL,
  amount NUMERIC(18,2) NOT NULL,
  result JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX journal_account_idx ON journal(account_id);
-- 2PC participant state: a COMMITTED row exists only if COMMIT happened (it is inserted
-- inside the prepared tx); an ABORTED tombstone blocks a late PREPARE.
CREATE TABLE tx_state (
  tx_id TEXT PRIMARY KEY,
  state TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Change capture used by live re-sharding (Bonus I).
CREATE TABLE changelog (
  seq BIGSERIAL PRIMARY KEY,
  xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
  account_id TEXT NOT NULL,
  balance NUMERIC(18,2) NOT NULL,
  version BIGINT NOT NULL
);
CREATE INDEX changelog_xid_seq_idx ON changelog(xid, seq);
CREATE FUNCTION accounts_capture() RETURNS trigger AS $$
BEGIN
  INSERT INTO changelog(account_id, balance, version) VALUES (NEW.id, NEW.balance, NEW.version);
  RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER accounts_capture_trg AFTER INSERT OR UPDATE ON accounts
  FOR EACH ROW EXECUTE FUNCTION accounts_capture();
