"""Banking shard replica. Stateless: all authoritative state lives in the shard's PostgreSQL.

Concurrency control (replicas of one shard share one DB):
  * every money-moving operation is ONE PostgreSQL transaction that takes row locks
    (SELECT ... FOR UPDATE, accounts always locked in sorted order => no deadlocks);
  * the shard DB serialises writers, so replicas can never produce lost updates;
  * a journal row (request_id PK) written in the same tx gives exactly-once semantics.
Ownership fencing: each write takes SELECT ... FOR SHARE on the account's chunk row and
checks status/epoch (enforced here, at the shard write path, not only in the gateway).
"""
import logging, socket, threading, time

import httpx, psycopg, uvicorn
from decimal import Decimal
from fastapi import FastAPI
from psycopg import sql
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from .common import (ApiError, check_amount, check_id, chunk_of, env, install_common, make_pool,
                     setup_logging)

REPLICA = env("REPLICA_NAME", socket.gethostname())
SHARD = env("SHARD", "?")
DB_URL = env("DB_URL")
GATEWAY = env("GATEWAY_URL", "http://gateway:8000")
LOCK_TIMEOUT = env("LOCK_TIMEOUT", "3s")

setup_logging()
log = logging.getLogger(REPLICA)
app = FastAPI(title=f"bank-{SHARD}")
install_common(app, REPLICA, header_name=REPLICA)
pool = None


class OpReq(BaseModel):
    amount: Decimal
    request_id: str
    epoch: int | None = None


class TransferReq(BaseModel):
    request_id: str
    src: str
    dst: str
    amount: Decimal
    epochs: dict[str, int] | None = None


class PrepareReq(BaseModel):
    tx_id: str
    account: str
    delta: Decimal
    epoch: int | None = None


class TxReq(BaseModel):
    tx_id: str


@app.on_event("startup")
def startup():
    global pool
    pool = make_pool(DB_URL, 20)
    threading.Thread(target=register_loop, daemon=True).start()
    log.info("replica %s of shard %s ready", REPLICA, SHARD)


def register_loop():
    """Self-registration / heartbeat to the gateway's replica registry."""
    while True:
        try:
            httpx.post(f"{GATEWAY}/admin/register", timeout=3,
                       json={"name": REPLICA, "shard": SHARD, "url": f"http://{REPLICA}:8000"})
        except Exception:
            pass
        time.sleep(5)


@app.get("/health")
def health():
    try:
        with pool.connection() as c:
            c.execute("SELECT 1")
    except Exception as e:
        raise ApiError(503, "DB_DOWN", str(e))
    return {"status": "ok", "replica": REPLICA, "shard": SHARD}


# ---------------------------------------------------------------- helpers
def check_owner(conn, account, epoch, lock):
    row = conn.execute("SELECT status, epoch FROM chunks WHERE chunk_id=%s" + (" FOR SHARE" if lock else ""),
                       (chunk_of(account),)).fetchone()
    if row is None or row["status"] == "INCOMING":
        raise ApiError(409, "NOT_OWNER", f"shard {SHARD} does not own {account}")
    if row["status"] != "ACTIVE":
        raise ApiError(409, "FENCED", f"{account} fenced on shard {SHARD} (status={row['status']}, epoch={row['epoch']})")
    if epoch is not None and epoch < row["epoch"]:
        raise ApiError(409, "STALE_EPOCH", f"request epoch {epoch} < shard epoch {row['epoch']}")


def run_tx(fn):
    """Run fn(conn) in one DB transaction; map DB errors to API errors; retry journal races."""
    for _ in range(2):
        with pool.connection() as conn:
            try:
                with conn.transaction():
                    conn.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
                    return fn(conn)
            except psycopg.errors.UniqueViolation:
                continue                      # concurrent duplicate request_id: re-run, journal replays it
            except psycopg.errors.LockNotAvailable:
                raise ApiError(503, "LOCK_TIMEOUT", "account locked by an in-flight transaction")
            except psycopg.errors.DeadlockDetected:
                raise ApiError(503, "DEADLOCK", "deadlock detected, retry")
    raise ApiError(503, "CONFLICT", "concurrent duplicate request, retry")


def replay(conn, request_id, account, op, amount):
    prior = conn.execute("SELECT account_id, op, amount, result FROM journal WHERE request_id=%s",
                         (request_id,)).fetchone()
    if not prior:
        return None
    if prior["account_id"] != account or prior["op"] != op or prior["amount"] != amount:
        raise ApiError(409, "REQUEST_ID_REUSED", "request_id already used for a different operation")
    r = dict(prior["result"])
    r["replayed"] = True
    return r


def journal(conn, request_id, account, op, amount, result):
    conn.execute("INSERT INTO journal(request_id,account_id,op,amount,result) VALUES (%s,%s,%s,%s,%s)",
                 (request_id, account, op, amount, Jsonb(result)))


def reply(d):
    d = dict(d)
    d["served_by"], d["shard"] = REPLICA, SHARD
    return d


# ---------------------------------------------------------------- local operations
@app.get("/accounts/{account}/balance")
def balance(account: str, epoch: int | None = None):
    with pool.connection() as conn:
        check_owner(conn, account, epoch, False)
        row = conn.execute("SELECT balance, version FROM accounts WHERE id=%s", (account,)).fetchone()
    if not row:
        raise ApiError(404, "ACCOUNT_NOT_FOUND", account)
    log.info("op=balance account=%s balance=%s", account, row["balance"])
    return reply({"account": account, "balance": str(row["balance"]), "version": row["version"]})


def balance_op(account, op, req: OpReq):
    check_id(req.request_id, "request_id")
    amount = check_amount(req.amount)

    def fn(conn):
        prior = replay(conn, req.request_id, account, op, amount)
        if prior:
            return prior
        check_owner(conn, account, req.epoch, True)
        row = conn.execute("SELECT balance FROM accounts WHERE id=%s FOR UPDATE", (account,)).fetchone()
        if not row:
            raise ApiError(404, "ACCOUNT_NOT_FOUND", account)
        delta = amount if op == "deposit" else -amount
        if row["balance"] + delta < 0:
            raise ApiError(422, "INSUFFICIENT_FUNDS", f"balance {row['balance']} < {amount}")
        new = conn.execute("UPDATE accounts SET balance=balance+%s, version=version+1 WHERE id=%s RETURNING balance",
                           (delta, account)).fetchone()["balance"]
        result = {"account": account, "op": op, "amount": str(amount), "balance": str(new)}
        journal(conn, req.request_id, account, op, amount, result)
        return result

    out = run_tx(fn)
    log.info("op=%s account=%s amount=%s req=%s replayed=%s", op, account, amount, req.request_id, out.get("replayed", False))
    return reply(out)


@app.post("/accounts/{account}/deposit")
def deposit(account: str, req: OpReq):
    return balance_op(account, "deposit", req)


@app.post("/accounts/{account}/withdraw")
def withdraw(account: str, req: OpReq):
    return balance_op(account, "withdraw", req)


@app.post("/transfer/local")
def transfer_local(req: TransferReq):
    """Same-shard transfer: ONE atomic local DB transaction, both accounts update or neither."""
    check_id(req.request_id, "request_id")
    amount = check_amount(req.amount)
    if req.src == req.dst:
        raise ApiError(400, "SAME_ACCOUNT", "source and destination are equal")
    epochs = req.epochs or {}

    def fn(conn):
        prior = replay(conn, req.request_id, req.src, "transfer", amount)
        if prior:
            return prior
        for a in sorted((req.src, req.dst)):
            check_owner(conn, a, epochs.get(a), True)
        rows = conn.execute("SELECT id, balance FROM accounts WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
                            ([req.src, req.dst],)).fetchall()
        bal = {r["id"]: r["balance"] for r in rows}
        for a in (req.src, req.dst):
            if a not in bal:
                raise ApiError(404, "ACCOUNT_NOT_FOUND", a)
        if bal[req.src] < amount:
            raise ApiError(422, "INSUFFICIENT_FUNDS", f"balance {bal[req.src]} < {amount}")
        conn.execute("UPDATE accounts SET balance=balance-%s, version=version+1 WHERE id=%s", (amount, req.src))
        conn.execute("UPDATE accounts SET balance=balance+%s, version=version+1 WHERE id=%s", (amount, req.dst))
        result = {"op": "transfer", "src": req.src, "dst": req.dst, "amount": str(amount),
                  "src_balance": str(bal[req.src] - amount), "dst_balance": str(bal[req.dst] + amount)}
        journal(conn, req.request_id, req.src, "transfer", amount, result)
        return result

    out = run_tx(fn)
    log.info("op=transfer_local %s->%s amount=%s req=%s replayed=%s", req.src, req.dst, amount, req.request_id, out.get("replayed", False))
    return reply(out)


# ---------------------------------------------------------------- 2PC participant
def prepared_exists(conn, gid):
    return conn.execute("SELECT 1 FROM pg_prepared_xacts WHERE gid=%s", (gid,)).fetchone() is not None


def committed_state(conn, gid):
    r = conn.execute("SELECT state FROM tx_state WHERE tx_id=%s", (gid,)).fetchone()
    return r["state"] if r else None


def rollback_quiet(conn):
    try:
        conn.execute("ROLLBACK")
    except Exception:
        pass


@app.post("/2pc/prepare")
def prepare(req: PrepareReq):
    """Validate, apply the change inside a transaction and PREPARE it (durable, not committed)."""
    gid = check_id(req.tx_id, "tx_id")
    delta = Decimal(req.delta)

    def vote(v, reason=None):
        log.info("2pc PREPARE gid=%s account=%s delta=%s -> %s %s", gid, req.account, delta, v, reason or "")
        return reply({"vote": v, "reason": reason, "tx_id": gid})

    with pool.connection() as conn:
        if prepared_exists(conn, gid):
            return vote("YES", "already prepared")
        st = committed_state(conn, gid)
        if st == "COMMITTED":
            return vote("YES", "already committed")
        if st == "ABORTED":
            return vote("NO", "ABORTED")
        try:
            conn.execute("BEGIN")
            conn.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
            conn.execute("INSERT INTO tx_state(tx_id,state) VALUES (%s,'COMMITTED')", (gid,))
            check_owner(conn, req.account, req.epoch, True)
            row = conn.execute("SELECT balance FROM accounts WHERE id=%s FOR UPDATE", (req.account,)).fetchone()
            if not row:
                raise ApiError(404, "ACCOUNT_NOT_FOUND", req.account)
            if row["balance"] + delta < 0:
                raise ApiError(422, "INSUFFICIENT_FUNDS", f"balance {row['balance']}")
            conn.execute("UPDATE accounts SET balance=balance+%s, version=version+1 WHERE id=%s", (delta, req.account))
            conn.execute("INSERT INTO journal(request_id,account_id,op,amount,result) VALUES (%s,%s,%s,%s,%s)",
                         (f"{gid}:{req.account}", req.account, "2pc", abs(delta), Jsonb({"op": "2pc", "delta": str(delta)})))
            conn.execute(sql.SQL("PREPARE TRANSACTION {}").format(sql.Literal(gid)))
            return vote("YES")
        except ApiError as e:
            rollback_quiet(conn)
            return vote("NO", e.code)
        except (psycopg.errors.LockNotAvailable, psycopg.errors.UniqueViolation, psycopg.errors.DeadlockDetected) as e:
            rollback_quiet(conn)
            if prepared_exists(conn, gid):             # concurrent duplicate PREPARE won
                return vote("YES", "already prepared")
            return vote("NO", "LOCK_TIMEOUT" if not isinstance(e, psycopg.errors.UniqueViolation) else "ABORTED")
        except Exception:
            rollback_quiet(conn)
            raise


@app.post("/2pc/commit")
def commit(req: TxReq):
    gid = check_id(req.tx_id, "tx_id")
    with pool.connection() as conn:
        try:
            conn.execute(sql.SQL("COMMIT PREPARED {}").format(sql.Literal(gid)))
            log.info("2pc COMMIT gid=%s done", gid)
            return reply({"status": "COMMITTED", "tx_id": gid})
        except psycopg.Error as e:
            if e.sqlstate != "42704":
                raise
        if committed_state(conn, gid) == "COMMITTED":        # duplicate COMMIT: idempotent
            log.info("2pc COMMIT gid=%s duplicate (already committed)", gid)
            return reply({"status": "COMMITTED", "tx_id": gid, "duplicate": True})
    raise ApiError(409, "NOT_PREPARED", f"{gid} is neither prepared nor committed")


@app.post("/2pc/abort")
def abort(req: TxReq):
    gid = check_id(req.tx_id, "tx_id")
    with pool.connection() as conn:
        for _ in range(3):
            if prepared_exists(conn, gid):
                try:
                    conn.execute(sql.SQL("ROLLBACK PREPARED {}").format(sql.Literal(gid)))
                except psycopg.Error as e:
                    if e.sqlstate != "42704":
                        raise
            try:
                with conn.transaction():
                    conn.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
                    conn.execute("INSERT INTO tx_state(tx_id,state) VALUES (%s,'ABORTED') ON CONFLICT DO NOTHING", (gid,))
                break
            except psycopg.errors.LockNotAvailable:
                continue                                       # a PREPARE for this gid is still in flight
        st = committed_state(conn, gid)
    if st == "COMMITTED":
        raise ApiError(409, "ALREADY_COMMITTED", gid)
    if st != "ABORTED":
        raise ApiError(503, "ABORT_PENDING", gid)
    log.info("2pc ABORT gid=%s done", gid)
    return reply({"status": "ABORTED", "tx_id": gid})


@app.get("/2pc/status/{gid}")
def twopc_status(gid: str):
    with pool.connection() as conn:
        return reply({"tx_id": gid, "prepared": prepared_exists(conn, gid), "state": committed_state(conn, gid)})


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False, log_level="warning")
