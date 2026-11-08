"""Helpers for the experiment scripts (stdlib only; needs `docker compose` on PATH)."""
import json, os, random, subprocess, sys, threading, time, urllib.error, urllib.request, uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
GW = os.getenv("GATEWAY_URL", "http://localhost:8000")
COORD, MIG, HEAL = "http://localhost:8100", "http://localhost:8200", "http://localhost:8300"
RESULTS = []


def http(method, url, body=None, timeout=45):
    req = urllib.request.Request(url, method=method, headers={"Content-Type": "application/json"},
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}"), {k.lower(): v for k, v in r.headers.items()}
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            j = json.loads(raw)
        except Exception:
            j = {"raw": raw.decode(errors="replace")}
        return e.code, j, {k.lower(): v for k, v in e.headers.items()}
    except Exception as e:
        return 0, {"error": "CONNECTION", "detail": str(e)}, {}


def rid():
    return uuid.uuid4().hex


def say(msg=""):
    print(msg, flush=True)


def title(t):
    say("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78)


def check(name, cond, detail=""):
    RESULTS.append(bool(cond))
    say(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))
    return cond


def summary():
    ok = all(RESULTS)
    say(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed -> {'SUCCESS' if ok else 'FAILURE'}")
    return 0 if ok else 1


def dc(*args, check_rc=False):
    r = subprocess.run(["docker", "compose", *args], capture_output=True, text=True)
    if check_rc and r.returncode:
        raise RuntimeError(r.stderr)
    return r


def psql(service, sql, db="bank"):
    r = subprocess.run(["docker", "compose", "exec", "-T", service, "psql", "-U", "bank", "-d", db, "-At", "-F", "|", "-c", sql],
                       capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f"psql {service}: {r.stderr}")
    return r.stdout.strip()


def running(service):
    return bool(dc("ps", "-q", service).stdout.strip())


def shard_services():
    return [s for s in ("db-a", "db-b", "db-c") if running(s)]


def db_balance(service, acct):
    v = psql(service, f"SELECT balance FROM accounts WHERE id='{acct}'")
    return Decimal(v) if v else None


def total_money():
    return sum(Decimal(psql(s, "SELECT coalesce(sum(balance),0) FROM accounts")) for s in shard_services())


def prepared_count():
    return sum(int(psql(s, "SELECT count(*) FROM pg_prepared_xacts")) for s in shard_services())


def reset_balances():
    for s in shard_services():
        psql(s, "UPDATE accounts SET balance=1000.00, version=version+1")


def wait_until(fn, timeout=60, interval=1.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            v = fn()
            if v:
                return v
        except Exception:
            pass
        time.sleep(interval)
    return None


def wait_platform(shards=("A", "B")):
    def ready():
        s, j, _ = http("GET", GW + "/admin/replicas", timeout=5)
        return s == 200 and all(any(r["shard"] == sh and r["healthy"] for r in j) for sh in shards)
    return wait_until(ready, 90)


# ---- bank operations through the gateway (client-side retries re-use the SAME request_id)
def op(kind, acct, amt, request_id=None, retries=8, pause=0.3):
    request_id = request_id or rid()
    s = j = h = None
    for i in range(retries):
        s, j, h = http("POST", f"{GW}/accounts/{acct}/{kind}", {"amount": str(amt), "request_id": request_id})
        if s in (200, 400, 404, 422):
            break
        time.sleep(pause * (i + 1))
    return s, j, h


def balance(acct):
    s, j, h = http("GET", f"{GW}/accounts/{acct}/balance")
    return (Decimal(j["balance"]) if s == 200 else None), j, h


def transfer(src, dst, amt, request_id=None, retries=8, pause=0.3):
    """Returns (final_status, body) where final_status in COMMITTED / ABORTED / UNKNOWN."""
    request_id = request_id or rid()
    body = None
    for i in range(retries):
        s, body, _ = http("POST", GW + "/transfer", {"src": src, "dst": dst, "amount": str(amt), "request_id": request_id})
        if s == 200 and (body.get("client_status") in (None, "COMMITTED")):
            return "COMMITTED", body
        if s in (400, 404, 422) or (s == 409 and body.get("client_status") == "ABORTED" and body.get("reason") not in
                                    ("STALE_EPOCH", "FENCED", "NOT_OWNER", "LOCK_TIMEOUT", "UNREACHABLE", "DEADLOCK")):
            return "ABORTED", body
        if s == 202 or s == 503 or s == 0 or s == 409:     # unknown outcome: ask the status API, then retry same id
            for _ in range(10):
                ss, st, _ = http("GET", f"{GW}/transactions/by-request/{request_id}")
                if st.get("client_status") == "COMMITTED":
                    return "COMMITTED", st
                if st.get("client_status") == "ABORTED" and st.get("reason") not in ("STALE_EPOCH", "FENCED", "NOT_OWNER", "LOCK_TIMEOUT", "UNREACHABLE", "DEADLOCK"):
                    return "ABORTED", st
                if st.get("client_status") in ("ABORTED", None, "UNKNOWN") and ss != 202:
                    break
                time.sleep(0.5)
        time.sleep(pause * (i + 1))
    return "UNKNOWN", body


def run_parallel(fn, items, workers=20):
    with ThreadPoolExecutor(workers) as ex:
        return list(ex.map(fn, items))


class Workload:
    """Background banking workload that records every effect that was acknowledged (exactly once)."""

    def __init__(self, ranges, workers=6, cross_partner_ranges=None):
        self.ranges, self.partners = ranges, cross_partner_ranges or ranges
        self.stop_evt, self.threads = threading.Event(), []
        self.lock, self.delta, self.counts = threading.Lock(), {}, {"ok": 0, "rejected": 0, "unknown": 0}
        self.workers, self.net_external = workers, Decimal(0)

    def pick(self, rs):
        lo, hi = random.choice(rs)
        return f"A{random.randint(lo, hi):04d}"

    def book(self, acct, d):
        with self.lock:
            self.delta[acct] = self.delta.get(acct, Decimal(0)) + d

    def one(self):
        r = random.random()
        amt = Decimal(random.randint(1, 20))
        a = self.pick(self.ranges)
        if r < 0.3:
            s, j, _ = op("deposit", a, amt, retries=40, pause=0.2)
            if s == 200:
                self.book(a, amt); self.net(amt); self.tick("ok")
            else:
                self.tick("unknown" if s != 422 else "rejected")
        elif r < 0.5:
            s, j, _ = op("withdraw", a, amt, retries=40, pause=0.2)
            if s == 200:
                self.book(a, -amt); self.net(-amt); self.tick("ok")
            else:
                self.tick("unknown" if s != 422 else "rejected")
        else:
            b = self.pick(self.partners)
            if a == b:
                return
            st, _ = transfer(a, b, amt, retries=40, pause=0.2)
            if st == "COMMITTED":
                self.book(a, -amt); self.book(b, amt); self.tick("ok")
            else:
                self.tick("unknown" if st == "UNKNOWN" else "rejected")

    def net(self, d):
        with self.lock:
            self.net_external += d

    def tick(self, k):
        with self.lock:
            self.counts[k] += 1

    def loop(self):
        while not self.stop_evt.is_set():
            self.one()

    def start(self):
        self.threads = [threading.Thread(target=self.loop, daemon=True) for _ in range(self.workers)]
        for t in self.threads:
            t.start()

    def stop(self):
        self.stop_evt.set()
        for t in self.threads:
            t.join(timeout=90)
