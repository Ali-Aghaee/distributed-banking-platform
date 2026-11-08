"""Transaction Coordinator: Two-Phase Commit with a durable decision log in PostgreSQL.

tx_log.status: INITIATED -> PREPARING -> PREPARED -> COMMIT_DECIDED -> COMPLETED(COMMITTED)
               (a NO vote / failure)  PREPARING -> ABORT_DECIDED -> COMPLETED(ABORTED)
Every transition is committed to the log BEFORE the corresponding message is sent. The same
`drive()` state machine serves live requests and crash recovery.
Recovery rules: PREPARING/INITIATED (no decision) -> ABORT (presumed abort);
                PREPARED (all YES) -> COMMIT; COMMIT_DECIDED -> always finish COMMIT; ABORT_DECIDED -> finish ABORT.
Participants are prepared sequentially in global account order (deadlock avoidance).
"""
import logging, os, random, threading, time, uuid

import httpx, psycopg, uvicorn
from decimal import Decimal
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from .common import (ApiError, MapCache, check_amount, check_id, env, install_common, make_pool,
                     registry_replicas, setup_logging)

setup_logging()
log = logging.getLogger("coordinator")
app = FastAPI(title="coordinator")
install_common(app, "coordinator")
META_URL = env("META_URL")
COMMIT_WAIT = float(env("COMMIT_WAIT", "8"))
MAX_ATTEMPTS = 5
RETRYABLE = {"STALE_EPOCH", "FENCED", "NOT_OWNER", "LOCK_TIMEOUT", "UNREACHABLE", "DEADLOCK"}
http = httpx.Client(timeout=httpx.Timeout(5.0, connect=1.5), limits=httpx.Limits(max_connections=200))
meta = maps = None
active, active_lock = set(), threading.Lock()
fault = {"point": None}
rep_cache = {}


class Unreachable(Exception):
    pass


def crash_point(name):
    if fault["point"] == name:
        log.error("FAULT INJECTION: coordinator crashing at '%s'", name)
        logging.shutdown()
        os._exit(1)


# ------------------------------------------------------------------ durable log helpers
def get_tx(tx_id):
    with meta.connection() as c:
        return c.execute("SELECT * FROM tx_log WHERE tx_id=%s", (tx_id,)).fetchone()


def event(c, tx_id, status, detail=""):
    c.execute("INSERT INTO tx_events(tx_id,status,detail) VALUES (%s,%s,%s)", (tx_id, status, detail))


def set_status(tx_id, status, outcome=None, reason=None):
    with meta.connection() as c, c.transaction():
        c.execute("UPDATE tx_log SET status=%s, outcome=COALESCE(%s,outcome), reason=COALESCE(%s,reason), updated_at=now() "
                  "WHERE tx_id=%s", (status, outcome, reason, tx_id))
        event(c, tx_id, status, reason or outcome or "")
    log.info("tx=%s -> %s %s", tx_id, status, reason or outcome or "")


def save_parts(tx_id, parts):
    with meta.connection() as c:
        c.execute("UPDATE tx_log SET participants=%s, updated_at=now() WHERE tx_id=%s", (Jsonb(parts), tx_id))


def claim(tx_id):
    with active_lock:
        if tx_id in active:
            return False
        active.add(tx_id)
        return True


def release(tx_id):
    with active_lock:
        active.discard(tx_id)


# ------------------------------------------------------------------ talking to participants
def shard_replicas(shard):
    now = time.monotonic()
    c = rep_cache.get(shard)
    if not c or now - c[0] > 1.0:
        rep_cache[shard] = c = (now, registry_replicas(meta, shard))
    return list(c[1])


def call(shard, path, body):
    reps = shard_replicas(shard)
    random.shuffle(reps)
    for r in reps:
        try:
            resp = http.post(r["url"] + path, json=body)
        except httpx.TransportError:
            continue
        if resp.status_code >= 500:                      # replica trouble: try another replica
            continue
        return resp
    raise Unreachable(shard)


def do_prepare(p):
    try:
        resp = call(p["shard"], "/2pc/prepare", {"tx_id": p["gid"], "account": p["account"], "delta": p["delta"], "epoch": p["epoch"]})
        d = resp.json()
        if d.get("vote") == "YES":
            return True, None
        return False, d.get("reason") or d.get("error") or "VOTE_NO"
    except Unreachable:
        return False, "UNREACHABLE"


def deliver(tx_id, kind, wait):
    """Send COMMIT/ABORT to every participant that has not acknowledged; True when all acked."""
    end = time.time() + wait
    first_ack = True
    while True:
        tx = get_tx(tx_id)
        parts = tx["participants"]
        for p in parts:
            if p["acked"]:
                continue
            try:
                resp = call(p["shard"], f"/2pc/{kind.lower()}", {"tx_id": p["gid"]})
                if resp.status_code == 200:
                    p["acked"] = True
                    save_parts(tx_id, parts)
                    log.info("tx=%s %s acked by shard %s", tx_id, kind, p["shard"])
                    if kind == "COMMIT" and first_ack:
                        first_ack = False
                        crash_point("mid_commit")
                else:
                    log.warning("tx=%s %s to shard %s -> %s %s", tx_id, kind, p["shard"], resp.status_code, resp.text[:120])
            except Unreachable:
                log.warning("tx=%s %s: shard %s unreachable, will retry", tx_id, kind, p["shard"])
        if all(p["acked"] for p in parts):
            set_status(tx_id, "COMPLETED", outcome="COMMITTED" if kind == "COMMIT" else "ABORTED")
            return True
        if time.time() > end:
            return False
        time.sleep(0.5)


def drive(tx_id, wait=COMMIT_WAIT, recovering=False):
    tx = get_tx(tx_id)
    st, parts = tx["status"], tx["participants"]
    if st in ("INITIATED", "PREPARING"):
        if recovering:                                        # no decision was logged -> presumed abort
            set_status(tx_id, "ABORT_DECIDED", reason="COORDINATOR_RECOVERY")
            st = "ABORT_DECIDED"
        else:
            if st == "INITIATED":
                set_status(tx_id, "PREPARING")
            abort_reason = None
            for i, p in enumerate(parts):                      # sequential, canonical order
                if p["vote"] is None:
                    ok, reason = do_prepare(p)
                    p["vote"], p["reason"] = ("YES" if ok else "NO"), reason
                    save_parts(tx_id, parts)
                    if i == 0:
                        crash_point("after_first_prepare")
                if p["vote"] == "NO":
                    abort_reason = p["reason"] or "VOTE_NO"
                    break
            if abort_reason:
                set_status(tx_id, "ABORT_DECIDED", reason=abort_reason)
                st = "ABORT_DECIDED"
            else:
                set_status(tx_id, "PREPARED")
                crash_point("after_prepare")
                st = "PREPARED"
    if st == "PREPARED":
        set_status(tx_id, "COMMIT_DECIDED")                   # decision durable BEFORE any COMMIT is sent
        crash_point("after_commit_decision")
        st = "COMMIT_DECIDED"
    if st == "COMMIT_DECIDED":
        deliver(tx_id, "COMMIT", wait)
    elif st == "ABORT_DECIDED":
        deliver(tx_id, "ABORT", min(wait, 4))
    return get_tx(tx_id)


# ------------------------------------------------------------------ API
def view(tx, events=False):
    done = tx["status"] == "COMPLETED"
    d = {"tx_id": tx["tx_id"], "request_id": tx["request_id"], "attempt": tx["attempt"], "status": tx["status"],
         "outcome": tx["outcome"], "client_status": tx["outcome"] if done else "PENDING", "reason": tx["reason"],
         "src": tx["src"], "dst": tx["dst"], "amount": str(tx["amount"]),
         "participants": [{k: p.get(k) for k in ("shard", "account", "delta", "vote", "reason", "acked")} for p in tx["participants"]]}
    if events:
        with meta.connection() as c:
            d["events"] = [{"status": e["status"], "detail": e["detail"], "ts": e["ts"].isoformat()} for e in
                           c.execute("SELECT status,detail,ts FROM tx_events WHERE tx_id=%s ORDER BY id", (tx["tx_id"],)).fetchall()]
    return d


def http_status(v):
    if v["client_status"] == "COMMITTED":
        return 200
    if v["client_status"] == "PENDING":
        return 202
    return {"INSUFFICIENT_FUNDS": 422, "ACCOUNT_NOT_FOUND": 404}.get(v["reason"], 409)


def latest_attempt(request_id):
    with meta.connection() as c:
        return c.execute("SELECT * FROM tx_log WHERE request_id=%s ORDER BY attempt DESC LIMIT 1", (request_id,)).fetchone()


class TransferReq(BaseModel):
    request_id: str
    src: str
    dst: str
    amount: Decimal


def build_parts(tx_id, src, dst, amount):
    sm = maps.get(force=True)
    (s1, e1), (s2, e2) = sm.lookup(src), sm.lookup(dst)
    parts = [{"shard": s1, "account": src, "delta": str(-amount), "epoch": e1},
             {"shard": s2, "account": dst, "delta": str(amount), "epoch": e2}]
    parts.sort(key=lambda p: p["account"])
    for i, p in enumerate(parts):
        p.update(gid=f"{tx_id}.{i}", vote=None, reason=None, acked=False)
    return parts


@app.post("/transfers")
def transfer(req: TransferReq):
    rid = check_id(req.request_id, "request_id")
    amount = check_amount(req.amount)
    if req.src == req.dst:
        raise ApiError(400, "SAME_ACCOUNT", "source and destination are equal")
    attempt = 1
    ex = latest_attempt(rid)
    if ex:
        if (ex["src"], ex["dst"], ex["amount"]) != (req.src, req.dst, amount):
            raise ApiError(409, "REQUEST_ID_REUSED", "request_id already used for a different transfer")
        if ex["status"] != "COMPLETED":
            for _ in range(20):                              # in flight in this process? wait a bit
                time.sleep(0.5)
                ex = get_tx(ex["tx_id"])
                if ex["status"] == "COMPLETED":
                    break
            v = view(ex)
            return JSONResponse(v, status_code=http_status(v))
        if ex["outcome"] == "COMMITTED" or ex["reason"] not in RETRYABLE:
            v = view(ex)
            return JSONResponse(dict(v, replayed=True), status_code=http_status(v))
        attempt = ex["attempt"] + 1                           # previous attempt aborted for a retryable reason
    tx = None
    for attempt in range(attempt, attempt + MAX_ATTEMPTS):
        tx_id = f"tx-{uuid.uuid4().hex[:16]}"
        claim(tx_id)
        try:
            parts = build_parts(tx_id, req.src, req.dst, amount)
            try:
                with meta.connection() as c, c.transaction():
                    c.execute("INSERT INTO tx_log(tx_id,request_id,attempt,src,dst,amount,status,participants) "
                              "VALUES (%s,%s,%s,%s,%s,%s,'INITIATED',%s)",
                              (tx_id, rid, attempt, req.src, req.dst, amount, Jsonb(parts)))
                    event(c, tx_id, "INITIATED", f"{req.src}->{req.dst} {amount}")
            except psycopg.errors.UniqueViolation:             # concurrent duplicate request
                time.sleep(1)
                v = view(latest_attempt(rid))
                return JSONResponse(v, status_code=http_status(v))
            log.info("tx=%s INITIATED %s -> %s amount=%s shards=%s", tx_id, req.src, req.dst, amount, [p["shard"] for p in parts])
            tx = drive(tx_id)
        finally:
            release(tx_id)
        if tx["status"] == "COMPLETED" and tx["outcome"] == "ABORTED" and tx["reason"] in RETRYABLE:
            time.sleep(random.uniform(0.1, 0.4) * (attempt % MAX_ATTEMPTS + 1))
            continue
        break
    v = view(tx)
    return JSONResponse(v, status_code=http_status(v))


@app.get("/transactions/by-request/{request_id}")
def by_request(request_id: str):
    tx = latest_attempt(request_id)
    if not tx:
        return JSONResponse({"status": "UNKNOWN", "client_status": "UNKNOWN", "request_id": request_id}, status_code=404)
    return view(tx, events=True)


@app.get("/transactions/{tx_id}")
def by_id(tx_id: str):
    tx = get_tx(tx_id)
    if not tx:
        return JSONResponse({"status": "UNKNOWN", "client_status": "UNKNOWN", "tx_id": tx_id}, status_code=404)
    return view(tx, events=True)


@app.get("/admin/recovery")
def recovery_info():
    with meta.connection() as c:
        return {"by_status": {r["status"]: r["n"] for r in c.execute("SELECT status, count(*) n FROM tx_log GROUP BY status").fetchall()},
                "unresolved": [r["tx_id"] for r in c.execute("SELECT tx_id FROM tx_log WHERE status<>'COMPLETED'").fetchall()]}


class FaultReq(BaseModel):
    point: str | None = None


@app.post("/admin/fault")
def set_fault(f: FaultReq):
    """Fault injection: after_first_prepare | after_prepare | after_commit_decision | mid_commit"""
    fault["point"] = f.point
    log.warning("fault injection armed: %s", f.point)
    return {"armed": f.point}


@app.get("/health")
def health():
    return {"status": "ok"}


def recovery_loop():
    first = True
    while True:
        try:
            with meta.connection() as c:
                ids = [r["tx_id"] for r in c.execute("SELECT tx_id FROM tx_log WHERE status<>'COMPLETED' ORDER BY created_at").fetchall()]
            if first and ids:
                log.warning("RECOVERY: found %d unresolved transaction(s): %s", len(ids), ids)
            for tx_id in ids:
                if claim(tx_id):
                    try:
                        before = get_tx(tx_id)["status"]
                        tx = drive(tx_id, wait=2, recovering=True)
                        log.info("RECOVERY tx=%s %s -> %s %s", tx_id, before, tx["status"], tx["outcome"] or "")
                    finally:
                        release(tx_id)
        except Exception as e:
            log.error("recovery loop error: %s", e)
        first = False
        time.sleep(2)


@app.on_event("startup")
def startup():
    global meta, maps
    meta = make_pool(META_URL, 30)
    maps = MapCache(meta)
    threading.Thread(target=recovery_loop, daemon=True).start()
    log.info("coordinator ready")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False, log_level="warning")
