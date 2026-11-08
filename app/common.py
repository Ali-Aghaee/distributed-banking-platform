"""Shared helpers: config, DB pools, shard map, errors, logging middleware."""
import logging, os, re, threading, time
from decimal import Decimal, InvalidOperation

import anyio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

CHUNK = 100                                   # accounts per ownership chunk
ROUTE_ERRORS = {"NOT_OWNER", "STALE_EPOCH", "FENCED"}
ACCT_RE = re.compile(r"^A(\d{4})$")
ID_RE = re.compile(r"^[A-Za-z0-9_.:#-]{1,100}$")


def env(name, default=None):
    return os.environ.get(name, default)


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


def make_pool(url, max_size=20):
    pool = ConnectionPool(url, min_size=1, max_size=max_size, open=False, timeout=15,
                          kwargs={"autocommit": True, "row_factory": dict_row})
    pool.open()
    pool.wait(timeout=60)
    return pool


class ApiError(Exception):
    def __init__(self, status, code, detail=""):
        super().__init__(f"{code}: {detail}")
        self.status, self.code, self.detail = status, code, detail


def install_common(app: FastAPI, service: str, header_name: str | None = None):
    log = logging.getLogger(service)

    @app.exception_handler(ApiError)
    async def _api_error(_req, exc: ApiError):
        return JSONResponse({"error": exc.code, "detail": exc.detail}, status_code=exc.status)

    @app.middleware("http")
    async def _mw(request: Request, call_next):
        t0 = time.time()
        resp = await call_next(request)
        if header_name:
            resp.headers["X-Served-By"] = header_name
        if request.url.path != "/health":
            log.info("%s %s -> %s (%.1f ms)", request.method, request.url.path, resp.status_code,
                     (time.time() - t0) * 1000)
        return resp

    @app.on_event("startup")
    async def _threads():
        anyio.to_thread.current_default_thread_limiter().total_tokens = 200


def acct_num(account: str) -> int:
    m = ACCT_RE.match(account or "")
    if not m or int(m.group(1)) < 1:
        raise ApiError(400, "BAD_ACCOUNT", f"invalid account id {account!r}")
    return int(m.group(1))


def chunk_of(account: str) -> int:
    return (acct_num(account) - 1) // CHUNK


def check_id(value: str, what="id"):
    if not value or not ID_RE.match(value):
        raise ApiError(400, "BAD_" + what.upper(), f"{what} must match {ID_RE.pattern}")
    return value


def check_amount(a) -> Decimal:
    try:
        d = Decimal(str(a))
    except InvalidOperation:
        raise ApiError(400, "BAD_AMOUNT", "not a number")
    if d != d.quantize(Decimal("0.01")) or d <= 0 or d > Decimal("1000000000"):
        raise ApiError(400, "BAD_AMOUNT", "amount must be > 0 with at most 2 decimals")
    return d.quantize(Decimal("0.01"))


class ShardMap:
    def __init__(self, rows, epoch):
        self.rows = [(r["lo"], r["hi"], r["shard"], r["epoch"]) for r in rows]
        self.epoch = epoch

    def lookup(self, account: str):
        n = acct_num(account)
        for lo, hi, shard, ep in self.rows:
            if lo <= n <= hi:
                return shard, ep
        raise ApiError(404, "ACCOUNT_NOT_FOUND", f"{account} is outside every shard range")

    def as_dict(self):
        return {"epoch": self.epoch,
                "ranges": [{"lo": l, "hi": h, "shard": s, "epoch": e} for l, h, s, e in self.rows]}


class MapCache:
    """Shard Map lives in the metadata DB; cached for `ttl` seconds."""

    def __init__(self, meta, ttl=1.0):
        self.meta, self.ttl, self.ts, self.value = meta, ttl, 0.0, None
        self.lock = threading.Lock()

    def get(self, force=False) -> ShardMap:
        with self.lock:
            if force or self.value is None or time.monotonic() - self.ts > self.ttl:
                with self.meta.connection() as c:
                    rows = c.execute("SELECT lo,hi,shard,epoch FROM shard_map ORDER BY lo").fetchall()
                    ep = c.execute("SELECT epoch FROM map_epoch").fetchone()["epoch"]
                self.value, self.ts = ShardMap(rows, ep), time.monotonic()
            return self.value


def registry_replicas(meta, shard=None):
    with meta.connection() as c:
        if shard:
            return c.execute("SELECT name,shard,url FROM replicas WHERE shard=%s ORDER BY name", (shard,)).fetchall()
        return c.execute("SELECT name,shard,url FROM replicas ORDER BY shard,name").fetchall()
