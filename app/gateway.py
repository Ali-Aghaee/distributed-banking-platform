"""Shard-aware Gateway.
 1) shard selection by account ownership (Shard Map in the metadata DB, with epochs)
 2) replica selection among HEALTHY replicas of that shard (least-connections, tie-break
    round-robin; LB_POLICY=round_robin switches policy)
Cross-shard transfers are delegated to the Transaction Coordinator.
"""
import logging, threading, time
from concurrent.futures import ThreadPoolExecutor

import httpx, uvicorn
from decimal import Decimal
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .common import (ROUTE_ERRORS, ApiError, MapCache, check_amount, check_id, env, install_common,
                     make_pool, registry_replicas, setup_logging)

setup_logging()
log = logging.getLogger("gateway")
app = FastAPI(title="gateway")
install_common(app, "gateway")
META_URL = env("META_URL")
COORD_URL = env("COORDINATOR_URL", "http://coordinator:8000")
POLICY = env("LB_POLICY", "least_conn")
http = httpx.Client(timeout=httpx.Timeout(10.0, connect=2.0), limits=httpx.Limits(max_connections=300, max_keepalive_connections=50))
meta = maps = None


class Replica:
    def __init__(self, name, shard, url):
        self.name, self.shard, self.url = name, shard, url
        self.healthy, self.fails, self.inflight, self.served, self.last = False, 0, 0, 0, 0


class Balancer:
    def __init__(self, policy):
        self.policy, self.lock, self.reps, self.tick = policy, threading.Lock(), {}, 0

    def sync(self, rows):
        with self.lock:
            names = {r["name"] for r in rows}
            for r in rows:
                cur = self.reps.get(r["name"])
                if cur is None or cur.url != r["url"]:
                    self.reps[r["name"]] = Replica(r["name"], r["shard"], r["url"])
            for n in list(self.reps):
                if n not in names:
                    del self.reps[n]

    def register(self, name, shard, url):
        with self.lock:
            if name not in self.reps:
                r = Replica(name, shard, url)
                r.healthy = True
                self.reps[name] = r
                log.info("replica %s (shard %s) registered", name, shard)

    def pick(self, shard, exclude):
        with self.lock:
            c = [r for r in self.reps.values() if r.shard == shard and r.healthy and r.name not in exclude]
            if not c:
                return None
            key = (lambda r: (r.inflight, r.last)) if self.policy == "least_conn" else (lambda r: r.last)
            r = min(c, key=key)
            self.tick += 1
            r.last, r.inflight = self.tick, r.inflight + 1
            return r

    def release(self, r, ok):
        with self.lock:
            r.inflight -= 1
            if ok:
                r.served += 1

    def mark_down(self, r):
        with self.lock:
            if r.healthy:
                log.warning("replica %s marked DOWN (connect failure)", r.name)
            r.healthy, r.fails = False, 2

    def health_result(self, r, ok):
        with self.lock:
            if ok:
                if not r.healthy:
                    log.info("replica %s is UP", r.name)
                r.healthy, r.fails = True, 0
            else:
                r.fails += 1
                if r.fails >= 2 and r.healthy:
                    r.healthy = False
                    log.warning("replica %s marked DOWN (health check failed)", r.name)

    def snapshot(self):
        with self.lock:
            return [{"name": r.name, "shard": r.shard, "url": r.url, "healthy": r.healthy,
                     "inflight": r.inflight, "served": r.served} for r in sorted(self.reps.values(), key=lambda x: x.name)]


bal = Balancer(POLICY)


def check_one(r):
    try:
        ok = httpx.get(r.url + "/health", timeout=1.0).status_code == 200
    except Exception:
        ok = False
    bal.health_result(r, ok)


def health_loop():
    pool = ThreadPoolExecutor(16)
    while True:
        try:
            bal.sync(registry_replicas(meta))
            with bal.lock:
                reps = list(bal.reps.values())
            list(pool.map(check_one, reps))
        except Exception as e:
            log.error("health loop: %s", e)
        time.sleep(1.0)


@app.on_event("startup")
def startup():
    global meta, maps
    meta = make_pool(META_URL, 10)
    maps = MapCache(meta)
    bal.sync(registry_replicas(meta))
    for r in list(bal.reps.values()):
        check_one(r)
    threading.Thread(target=health_loop, daemon=True).start()
    log.info("gateway ready, policy=%s", POLICY)


@app.get("/health")
def health():
    return {"status": "ok"}


# ------------------------------------------------------------------ forwarding
def call_shard(shard, method, path, body=None, params=None):
    tried = set()
    while True:
        r = bal.pick(shard, tried)
        if r is None:
            raise ApiError(503, "NO_HEALTHY_REPLICA", f"shard {shard} has no healthy replica")
        try:
            resp = http.request(method, r.url + path, json=body, params=params)
            bal.release(r, True)
            return resp, r
        except httpx.ConnectError:
            bal.release(r, False)
            bal.mark_down(r)
            tried.add(r.name)
        except httpx.TransportError as e:
            bal.release(r, False)
            log.warning("replica %s transport error %s, retrying on another replica", r.name, type(e).__name__)
            tried.add(r.name)


def out(resp, r, extra=None):
    try:
        body = resp.json()
    except Exception:
        body = {"error": "BAD_BACKEND_RESPONSE", "detail": resp.text[:200]}
    return JSONResponse(body, status_code=resp.status_code, headers={"X-Served-By": r.name if r else "gateway"})


def routed(account, method, path_fn, body_fn=None, params_fn=None):
    """Resolve owner shard via the Shard Map; on stale-route rejections refresh the map and retry."""
    for attempt in range(10):
        shard, epoch = maps.get().lookup(account)
        resp, r = call_shard(shard, method, path_fn(), body_fn(epoch) if body_fn else None,
                             params_fn(epoch) if params_fn else None)
        log.info("route account=%s shard=%s epoch=%s replica=%s status=%s attempt=%d policy=%s",
                 account, shard, epoch, r.name, resp.status_code, attempt, POLICY)
        if resp.status_code == 409 and resp.json().get("error") in ROUTE_ERRORS:
            maps.get(force=True)
            time.sleep(0.15 * (attempt + 1))
            continue
        return out(resp, r)
    raise ApiError(503, "ROUTE_UNSTABLE", "ownership of the account is changing, retry")


class OpBody(BaseModel):
    amount: Decimal
    request_id: str


class TransferBody(BaseModel):
    src: str
    dst: str
    amount: Decimal
    request_id: str


class RegisterBody(BaseModel):
    name: str
    shard: str
    url: str = ""


class DirectBody(BaseModel):
    shard: str
    op: str = "deposit"
    account: str
    amount: Decimal = Decimal("1")
    epoch: int | None = None
    request_id: str | None = None


@app.get("/accounts/{account}/balance")
def balance(account: str):
    return routed(account, "GET", lambda: f"/accounts/{account}/balance", params_fn=lambda e: {"epoch": e})


def money_op(account, op, b: OpBody):
    check_id(b.request_id, "request_id")
    amount = check_amount(b.amount)
    return routed(account, "POST", lambda: f"/accounts/{account}/{op}",
                  lambda e: {"amount": str(amount), "request_id": b.request_id, "epoch": e})


@app.post("/accounts/{account}/deposit")
def deposit(account: str, b: OpBody):
    return money_op(account, "deposit", b)


@app.post("/accounts/{account}/withdraw")
def withdraw(account: str, b: OpBody):
    return money_op(account, "withdraw", b)


@app.post("/transfer")
def transfer(b: TransferBody):
    check_id(b.request_id, "request_id")
    amount = check_amount(b.amount)
    for attempt in range(10):
        sm = maps.get()
        (s1, e1), (s2, e2) = sm.lookup(b.src), sm.lookup(b.dst)
        if s1 == s2:                                   # same shard: one local DB transaction, no 2PC
            resp, r = call_shard(s1, "POST", "/transfer/local",
                                 {"request_id": b.request_id, "src": b.src, "dst": b.dst, "amount": str(amount),
                                  "epochs": {b.src: e1, b.dst: e2}})
            log.info("transfer %s->%s LOCAL shard=%s replica=%s status=%s", b.src, b.dst, s1, r.name, resp.status_code)
            if resp.status_code == 409 and resp.json().get("error") in ROUTE_ERRORS:
                maps.get(force=True)
                time.sleep(0.15 * (attempt + 1))
                continue
            return out(resp, r)
        try:                                           # cross-shard: Two-Phase Commit via the coordinator
            resp = httpx.post(COORD_URL + "/transfers", timeout=40,
                              json={"request_id": b.request_id, "src": b.src, "dst": b.dst, "amount": str(amount)})
        except httpx.HTTPError as e:
            return JSONResponse({"error": "COORDINATOR_UNAVAILABLE", "status": "UNKNOWN",
                                 "request_id": b.request_id, "detail": type(e).__name__,
                                 "hint": f"GET /transactions/by-request/{b.request_id}"}, status_code=503)
        log.info("transfer %s->%s CROSS-SHARD (%s,%s) via coordinator status=%s", b.src, b.dst, s1, s2, resp.status_code)
        return JSONResponse(resp.json(), status_code=resp.status_code, headers={"X-Served-By": "coordinator"})
    raise ApiError(503, "ROUTE_UNSTABLE", "ownership is changing, retry")


def coord_get(path):
    try:
        resp = httpx.get(COORD_URL + path, timeout=5)
        return JSONResponse(resp.json(), status_code=resp.status_code)
    except httpx.HTTPError:
        return JSONResponse({"status": "UNKNOWN", "client_status": "UNKNOWN", "error": "COORDINATOR_UNAVAILABLE"}, status_code=503)


@app.get("/transactions/by-request/{request_id}")
def tx_by_request(request_id: str):
    return coord_get(f"/transactions/by-request/{request_id}")


@app.get("/transactions/{tx_id}")
def tx_status(tx_id: str):
    return coord_get(f"/transactions/{tx_id}")


# ------------------------------------------------------------------ admin
@app.post("/admin/register")
def register(b: RegisterBody):
    with meta.connection() as c:
        c.execute("INSERT INTO replicas(name,shard,url) VALUES (%s,%s,%s) ON CONFLICT (name) DO UPDATE "
                  "SET shard=EXCLUDED.shard, url=EXCLUDED.url, last_seen=now()", (b.name, b.shard, b.url))
    bal.register(b.name, b.shard, b.url)
    return {"ok": True}


@app.post("/admin/deregister")
def deregister(b: RegisterBody):
    with meta.connection() as c:
        c.execute("DELETE FROM replicas WHERE name=%s", (b.name,))
    bal.sync(registry_replicas(meta))
    log.warning("replica %s deregistered", b.name)
    return {"ok": True}


@app.get("/admin/replicas")
def replicas():
    return bal.snapshot()


@app.get("/admin/shard-map")
def shard_map():
    return maps.get(force=True).as_dict()


@app.post("/debug/direct")
def direct(b: DirectBody):
    """TEST HOOK: send a write straight to a shard's replica with an arbitrary epoch (stale-route proof)."""
    import uuid
    resp, r = call_shard(b.shard, "POST", f"/accounts/{b.account}/{b.op}",
                         {"amount": str(b.amount), "request_id": b.request_id or uuid.uuid4().hex, "epoch": b.epoch})
    return out(resp, r)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False, log_level="warning")
