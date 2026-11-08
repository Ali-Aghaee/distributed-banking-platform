"""Live re-sharding (Bonus I): moves a chunk-aligned account range between shards while the
platform keeps serving traffic. Crash-consistent state machine persisted in the metadata DB.

PLANNED -> SNAPSHOT -> DELTA -> FENCING -> ACTIVATING -> MAP_ACTIVATED -> CLEANUP -> DONE   (or FAILED)

* SNAPSHOT : REPEATABLE READ copy in key batches, durable cursor, idempotent version-guarded upserts.
* DELTA    : replay of the trigger-based `changelog` ordered by (xid, seq); only entries with
             xid < snapshot-xmin are final, so no committed change can be skipped. Durable (xid,seq) checkpoint.
* FENCING  : UPDATE chunks SET status='FENCED', epoch=new on the OLD shard. The UPDATE waits for in-flight
             writers (incl. prepared 2PC legs hold FOR SHARE locks) => old owner can no longer be modified;
             then the final drain, journal copy and checksum verification run on a quiescent range.
* ACTIVATING: new shard chunks ACTIVE@epoch, then ONE metadata transaction flips the Shard Map (+epoch).
* CLEANUP  : verified, then old data deleted; old chunks stay MOVED forever (permanent fence).
"""
import logging, os, threading, time

import psycopg, uvicorn
from fastapi import FastAPI
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from .common import ApiError, CHUNK, env, install_common, make_pool, setup_logging

setup_logging()
log = logging.getLogger("migrator")
app = FastAPI(title="migrator")
install_common(app, "migrator")
META_URL = env("META_URL")
LAG_THRESHOLD, DELTA_MAX_SECONDS, BATCH = 5, 60, 40
meta = None
fault = {"phase": None}

UPSERT = ("INSERT INTO accounts(id,balance,version) VALUES (%s,%s,%s) ON CONFLICT (id) DO UPDATE "
          "SET balance=EXCLUDED.balance, version=EXCLUDED.version WHERE accounts.version < EXCLUDED.version")


def crash(point):
    if fault["phase"] == point:
        log.error("FAULT INJECTION: migrator killed at '%s'", point)
        logging.shutdown()
        os._exit(1)


def connect(shard):
    with meta.connection() as c:
        url = c.execute("SELECT db_url FROM shards WHERE shard=%s", (shard,)).fetchone()["db_url"]
    return psycopg.connect(url, autocommit=True, row_factory=dict_row)


def mget(mid):
    with meta.connection() as c:
        return c.execute("SELECT * FROM migration WHERE id=%s", (mid,)).fetchone()


def mset(mid, **cols):
    sets = ", ".join(f"{k}=%s" for k in cols)
    with meta.connection() as c:
        c.execute(f"UPDATE migration SET {sets}, updated_at=now() WHERE id=%s", (*cols.values(), mid))


def bounds(m):
    return f"A{m['lo']:04d}", f"A{m['hi']:04d}", list(range((m["lo"] - 1) // CHUNK, (m["hi"] - 1) // CHUNK + 1))


def upsert(dst, rows):
    with dst.transaction():
        for r in rows:
            dst.execute(UPSERT, (r.get("account_id", r.get("id")), r["balance"], r["version"]))


def current_xmin(conn):
    return int(conn.execute("SELECT pg_snapshot_xmin(pg_current_snapshot())::text AS x").fetchone()["x"])


# ---------------------------------------------------------------- phases
def p_planned(m, src, dst):
    lo_id, hi_id, cids = bounds(m)
    rows = src.execute("SELECT chunk_id,status FROM chunks WHERE chunk_id=ANY(%s)", (cids,)).fetchall()
    if len(rows) != len(cids) or any(r["status"] != "ACTIVE" for r in rows):
        raise RuntimeError(f"source chunks not all ACTIVE: {rows}")
    with dst.transaction():
        for c in cids:
            dst.execute("INSERT INTO chunks(chunk_id,lo,hi,status,epoch) VALUES (%s,%s,%s,'INCOMING',0) ON CONFLICT DO NOTHING",
                        (c, c * CHUNK + 1, c * CHUNK + CHUNK))
    with meta.connection() as c:
        c.execute("UPDATE shards SET enabled=TRUE WHERE shard=%s", (m["dst"],))   # healer starts managing the new shard
    mset(m["id"], phase="SNAPSHOT")


def p_snapshot(m, src, dst):
    lo_id, hi_id, _ = bounds(m)
    src.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
    try:
        xmin = current_xmin(src)
        if m["start_xid"] is None:        # persisted BEFORE copying: delta replay starts here
            mset(m["id"], start_xid=xmin, delta_xid=xmin - 1, delta_seq=0)
        cursor, batches = m["snapshot_cursor"], 0
        while True:
            rows = src.execute("SELECT id,balance,version FROM accounts WHERE id>%s AND id BETWEEN %s AND %s ORDER BY id LIMIT %s",
                               (cursor, lo_id, hi_id, BATCH)).fetchall()
            if not rows:
                break
            upsert(dst, rows)
            cursor = rows[-1]["id"]
            mset(m["id"], snapshot_cursor=cursor)
            batches += 1
            log.info("migration %s SNAPSHOT copied up to %s", m["id"], cursor)
            if batches == 1:
                crash("SNAPSHOT")
    finally:
        src.execute("COMMIT")
    mset(m["id"], phase="DELTA")


def replay(m, src, dst, barrier):
    lo_id, hi_id, _ = bounds(m)
    total = 0
    while True:
        cur = mget(m["id"])
        q = ("SELECT seq, xid::text AS xid, account_id, balance, version FROM changelog "
             "WHERE (xid, seq) > (%s::text::xid8, %s) " + ("AND xid < %s::text::xid8 " if barrier is not None else "") +
             "AND account_id BETWEEN %s AND %s ORDER BY xid, seq LIMIT 500")
        args = [str(cur["delta_xid"]), cur["delta_seq"]] + ([str(barrier)] if barrier is not None else []) + [lo_id, hi_id]
        rows = src.execute(q, args).fetchall()
        if not rows:
            return total
        upsert(dst, rows)                                        # idempotent: version-guarded
        mset(m["id"], delta_xid=int(rows[-1]["xid"]), delta_seq=rows[-1]["seq"])
        total += len(rows)


def p_delta(m, src, dst):
    t0, stable, first = time.time(), 0, True
    while True:
        n = replay(m, src, dst, current_xmin(src))
        log.info("migration %s DELTA replayed %d change(s)", m["id"], n)
        if first:
            first = False
            crash("DELTA")
        stable = stable + 1 if n <= LAG_THRESHOLD else 0
        if stable >= 2 or time.time() - t0 > DELTA_MAX_SECONDS:
            break
        time.sleep(0.5)
    mset(m["id"], phase="FENCING")


def summary(conn, lo_id, hi_id):
    r = conn.execute("SELECT count(*) n, coalesce(sum(balance),0) b, coalesce(sum(version),0) v FROM accounts WHERE id BETWEEN %s AND %s",
                     (lo_id, hi_id)).fetchone()
    return (r["n"], r["b"], r["v"])


def copy_journal(m, src, dst):
    lo_id, hi_id, _ = bounds(m)
    cursor = ""
    while True:
        rows = src.execute("SELECT request_id,account_id,op,amount,result,created_at FROM journal "
                           "WHERE request_id>%s AND account_id BETWEEN %s AND %s ORDER BY request_id LIMIT 500",
                           (cursor, lo_id, hi_id)).fetchall()
        if not rows:
            return
        with dst.transaction():
            for r in rows:
                dst.execute("INSERT INTO journal(request_id,account_id,op,amount,result,created_at) VALUES (%s,%s,%s,%s,%s,%s) "
                            "ON CONFLICT DO NOTHING", (r["request_id"], r["account_id"], r["op"], r["amount"], Jsonb(r["result"]), r["created_at"]))
        cursor = rows[-1]["request_id"]


def p_fencing(m, src, dst):
    lo_id, hi_id, cids = bounds(m)
    while True:                                           # wait for in-flight writers / prepared txs, 3s at a time
        try:
            with src.transaction():
                src.execute("SET LOCAL lock_timeout='3s'")
                src.execute("UPDATE chunks SET status='FENCED', epoch=%s WHERE chunk_id=ANY(%s) AND epoch<%s",
                            (m["new_epoch"], cids, m["new_epoch"]))
            break
        except psycopg.errors.LockNotAvailable:
            log.warning("migration %s: fence waiting for in-flight transactions (prepared 2PC legs?)", m["id"])
    log.info("migration %s: old owner FENCED at epoch %s", m["id"], m["new_epoch"])
    crash("FENCING")
    for _ in range(3):                                    # quiescent range: final drain + journal + checksum
        replay(m, src, dst, None)
        copy_journal(m, src, dst)
        a, b = summary(src, lo_id, hi_id), summary(dst, lo_id, hi_id)
        if a == b:
            log.info("migration %s: verified %s accounts, checksum match", m["id"], a[0])
            mset(m["id"], phase="ACTIVATING")
            return
        log.warning("migration %s: checksum mismatch src=%s dst=%s, re-draining", m["id"], a, b)
    rollback(m, src, f"verification failed src={a} dst={b}")


def flip_map(c, m, owner, epoch):
    lo, hi = m["lo"], m["hi"]
    cur = c.execute("SELECT lo,hi,shard,epoch FROM shard_map WHERE lo<=%s AND hi>=%s FOR UPDATE", (lo, hi)).fetchone()
    if not cur:
        raise RuntimeError("range not inside one shard-map entry")
    c.execute("DELETE FROM shard_map WHERE lo=%s", (cur["lo"],))
    pieces = []
    if cur["lo"] < lo:
        pieces.append((cur["lo"], lo - 1, cur["shard"], cur["epoch"]))
    pieces.append((lo, hi, owner, epoch))
    if hi < cur["hi"]:
        pieces.append((hi + 1, cur["hi"], cur["shard"], cur["epoch"]))
    for p in pieces:
        c.execute("INSERT INTO shard_map(lo,hi,shard,epoch) VALUES (%s,%s,%s,%s)", p)
    c.execute("UPDATE map_epoch SET epoch=GREATEST(epoch,%s)", (epoch,))


def rollback(m, src, reason):
    _, _, cids = bounds(m)
    src.execute("UPDATE chunks SET status='ACTIVE' WHERE chunk_id=ANY(%s) AND epoch=%s AND status='FENCED'", (cids, m["new_epoch"]))
    with meta.connection() as c, c.transaction():
        flip_map(c, m, m["src"], m["new_epoch"])
        c.execute("UPDATE migration SET phase='FAILED', info=%s, updated_at=now() WHERE id=%s", (reason, m["id"]))
    log.error("migration %s ROLLED BACK: %s", m["id"], reason)


def p_activating(m, src, dst):
    _, _, cids = bounds(m)
    dst.execute("UPDATE chunks SET status='ACTIVE', epoch=%s WHERE chunk_id=ANY(%s) AND epoch<=%s", (m["new_epoch"], cids, m["new_epoch"]))
    with meta.connection() as c, c.transaction():          # atomic cutover decision
        flip_map(c, m, m["dst"], m["new_epoch"])
        c.execute("UPDATE migration SET phase='MAP_ACTIVATED', updated_at=now() WHERE id=%s", (m["id"],))
    log.info("migration %s: shard map flipped to epoch %s (%s now owned by %s)", m["id"], m["new_epoch"], f"A{m['lo']:04d}-A{m['hi']:04d}", m["dst"])
    crash("MAP_ACTIVATED")


def p_map_activated(m, src, dst):
    mset(m["id"], phase="CLEANUP")


def p_cleanup(m, src, dst):
    lo_id, hi_id, cids = bounds(m)
    cursor = ""
    while True:                                           # old data is removed only after verified completion
        rows = src.execute("SELECT id,balance,version FROM accounts WHERE id>%s AND id BETWEEN %s AND %s ORDER BY id LIMIT 200",
                           (cursor, lo_id, hi_id)).fetchall()
        if not rows:
            break
        got = {r["id"]: r for r in dst.execute("SELECT id,balance,version FROM accounts WHERE id=ANY(%s)", ([r["id"] for r in rows],)).fetchall()}
        for r in rows:
            d = got.get(r["id"])
            if d is None or d["version"] < r["version"] or (d["version"] == r["version"] and d["balance"] != r["balance"]):
                mset(m["id"], phase="FAILED", info=f"cleanup verification failed for {r['id']}; old data kept")
                log.error("migration %s cleanup verification failed for %s", m["id"], r["id"])
                return
        cursor = rows[-1]["id"]
    with src.transaction():
        src.execute("DELETE FROM accounts WHERE id BETWEEN %s AND %s", (lo_id, hi_id))
        src.execute("DELETE FROM journal WHERE account_id BETWEEN %s AND %s", (lo_id, hi_id))
        src.execute("DELETE FROM changelog WHERE account_id BETWEEN %s AND %s", (lo_id, hi_id))
        src.execute("UPDATE chunks SET status='MOVED' WHERE chunk_id=ANY(%s)", (cids,))
    mset(m["id"], phase="DONE")
    log.info("migration %s DONE (old data removed, old chunks permanently fenced)", m["id"])


PHASES = {"PLANNED": p_planned, "SNAPSHOT": p_snapshot, "DELTA": p_delta, "FENCING": p_fencing,
          "ACTIVATING": p_activating, "MAP_ACTIVATED": p_map_activated, "CLEANUP": p_cleanup}


def supervise(mid):
    while True:
        try:
            m = mget(mid)
            src, dst = connect(m["src"]), connect(m["dst"])
            try:
                while True:
                    m = mget(mid)
                    if m["phase"] in ("DONE", "FAILED"):
                        return
                    log.info("migration %s phase=%s", mid, m["phase"])
                    PHASES[m["phase"]](m, src, dst)
            finally:
                src.close(); dst.close()
        except Exception as e:
            log.error("migration %s error: %s (retrying)", mid, e)
            time.sleep(2)


# ---------------------------------------------------------------- API
class MigrationReq(BaseModel):
    src: str
    dst: str
    lo: int
    hi: int


@app.post("/migrations")
def start(r: MigrationReq):
    if r.src == r.dst or (r.lo - 1) % CHUNK or r.hi % CHUNK or r.lo > r.hi:
        raise ApiError(400, "BAD_RANGE", f"range must be chunk-aligned ({CHUNK} accounts) and src != dst")
    with meta.connection() as c:
        if c.execute("SELECT 1 FROM migration WHERE phase NOT IN ('DONE','FAILED')").fetchone():
            raise ApiError(409, "MIGRATION_RUNNING", "only one migration at a time")
        cur = c.execute("SELECT shard FROM shard_map WHERE lo<=%s AND hi>=%s", (r.lo, r.hi)).fetchone()
        if not cur or cur["shard"] != r.src:
            raise ApiError(409, "NOT_OWNED_BY_SRC", f"range is not wholly owned by {r.src}")
        ep = c.execute("SELECT epoch FROM map_epoch").fetchone()["epoch"]
        mid = c.execute("INSERT INTO migration(src,dst,lo,hi,new_epoch,phase) VALUES (%s,%s,%s,%s,%s,'PLANNED') RETURNING id",
                        (r.src, r.dst, r.lo, r.hi, ep + 1)).fetchone()["id"]
    threading.Thread(target=supervise, args=(mid,), daemon=True).start()
    return mget(mid)


@app.get("/migrations/{mid}")
def get_one(mid: int):
    m = mget(mid)
    if not m:
        raise ApiError(404, "NOT_FOUND", str(mid))
    return {k: (str(v) if k.endswith("_at") else v) for k, v in m.items()}


@app.get("/migrations")
def list_all():
    with meta.connection() as c:
        return [{k: (str(v) if k.endswith("_at") else v) for k, v in r.items()} for r in c.execute("SELECT * FROM migration ORDER BY id").fetchall()]


class FaultReq(BaseModel):
    phase: str | None = None


@app.post("/admin/fault")
def set_fault(f: FaultReq):
    """Kill the migrator at: SNAPSHOT | DELTA | FENCING | MAP_ACTIVATED"""
    fault["phase"] = f.phase
    log.warning("fault injection armed: %s", f.phase)
    return {"armed": f.phase}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.on_event("startup")
def startup():
    global meta
    meta = make_pool(META_URL, 10)
    with meta.connection() as c:
        pending = [r["id"] for r in c.execute("SELECT id FROM migration WHERE phase NOT IN ('DONE','FAILED') ORDER BY id").fetchall()]
    for mid in pending:
        log.warning("RECOVERY: resuming unfinished migration %s from its persisted phase", mid)
        threading.Thread(target=supervise, args=(mid,), daemon=True).start()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False, log_level="warning")
