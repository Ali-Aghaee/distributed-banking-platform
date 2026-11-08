from lib import *

title("Experiment 6 - Interrupted distributed transactions (Coordinator crash & recovery)")
wait_platform(); reset_balances()
base_total = total_money()

def arm(point):
    s, _, _ = http("POST", COORD + "/admin/fault", {"point": point})
    assert s == 200

def fire(src, dst, amt, r):
    out = {}
    t = threading.Thread(target=lambda: out.update(zip(("s", "j"), http("POST", GW + "/transfer", {"src": src, "dst": dst, "amount": str(amt), "request_id": r}, timeout=40)[:2])))
    t.start(); t.join()
    return out

def row(r):
    return psql("db-coord", f"SELECT status||'/'||coalesce(outcome,'-')||'/'||coalesce(reason,'-') FROM tx_log WHERE request_id='{r}' ORDER BY attempt DESC LIMIT 1")

def coord_up():
    return http("GET", COORD + "/health", timeout=3)[0] == 200

def restart_coordinator():
    dc("start", "coordinator")
    assert wait_until(coord_up, 40), "coordinator did not come back"

def coord_dead():
    return not coord_up()

# ---------------------------------------------------------------- main experiment
say("\n### 6.1 COMMIT durably recorded, Coordinator killed before ANY participant received COMMIT")
r = rid(); arm("after_commit_decision")
res = fire("A0100", "A1100", 50, r)
check("coordinator process died during the request", coord_dead())
check("durable log says COMMIT_DECIDED", row(r).startswith("COMMIT_DECIDED"), row(r))
check("both shards hold a PREPARED (uncommitted) transaction", prepared_count() == 2)
check("committed balances are still the OLD ones (nothing half-applied)", (db_balance("db-a", "A0100"), db_balance("db-b", "A1100")) == (1000, 1000))
s, j, _ = http("GET", f"{GW}/transactions/by-request/{r}")
check("client asking the gateway gets UNKNOWN (never a false success)", j.get("client_status") == "UNKNOWN", f"HTTP {s}")
s, j, _ = http("POST", f"{GW}/accounts/A0100/deposit", {"amount": "1", "request_id": rid()})
check("another writer is blocked by the prepared tx's lock (503 LOCK_TIMEOUT) instead of corrupting it", s == 503 and j.get("error") == "LOCK_TIMEOUT", f"HTTP {s} {j.get('error')}")
say("restarting coordinator ...")
restart_coordinator()
ok = wait_until(lambda: row(r).startswith("COMPLETED/COMMITTED"), 40)
check("after restart the recorded COMMIT is completed", bool(ok), row(r))
check("balances 950 / 1050", (db_balance("db-a", "A0100"), db_balance("db-b", "A1100")) == (950, 1050))
check("no prepared transactions remain", prepared_count() == 0)
s, j, _ = http("GET", f"{GW}/transactions/by-request/{r}")
check("status API now reports COMMITTED", j.get("client_status") == "COMMITTED")
say("recovery trail: " + " -> ".join(e["status"] for e in j["events"]))

# ---------------------------------------------------------------- scenario B
say("\n### 6.2 (Scenario B) all participants PREPARED, Coordinator stops before deciding")
r = rid(); arm("after_prepare"); fire("A0200", "A1200", 30, r)
check("log says PREPARED, two prepared txs in the shards", row(r).startswith("PREPARED") and prepared_count() == 2, row(r))
restart_coordinator()
check("recovered from durable state and COMMITTED", bool(wait_until(lambda: row(r).startswith("COMPLETED/COMMITTED"), 40)), row(r))
check("balances 970 / 1030", (db_balance("db-a", "A0200"), db_balance("db-b", "A1200")) == (970, 1030))

# ---------------------------------------------------------------- scenario C
say("\n### 6.3 (Scenario C) COMMIT recorded, one shard completely DOWN -> must never turn into ABORT")
r = rid(); arm("after_commit_decision"); fire("A0300", "A1300", 20, r)
dc("stop", "bank-b1", "bank-b2")
restart_coordinator()
time.sleep(10)
check("transaction stays COMMIT_DECIDED while shard B is down (no ABORT)", row(r).startswith("COMMIT_DECIDED"), row(r))
s, j, _ = http("GET", f"{GW}/transactions/by-request/{r}")
check("status API exposes PENDING", j.get("client_status") == "PENDING", str(j.get("client_status")))
check("shard A leg already committed, shard B leg still prepared", (db_balance("db-a", "A0300"), db_balance("db-b", "A1300"), prepared_count()) == (980, 1000, 1))
say("starting shard B replicas again ...")
dc("start", "bank-b1", "bank-b2"); wait_platform()
check("coordinator completed the recorded COMMIT", bool(wait_until(lambda: row(r).startswith("COMPLETED/COMMITTED"), 60)), row(r))
check("balances 980 / 1020, nothing prepared", (db_balance("db-a", "A0300"), db_balance("db-b", "A1300"), prepared_count()) == (980, 1020, 0))

# ---------------------------------------------------------------- crash while still preparing
say("\n### 6.4 Coordinator crash with NO decision yet (after first PREPARE) -> safe ABORT on recovery")
r = rid(); arm("after_first_prepare"); fire("A0400", "A1400", 40, r)
check("one leg is prepared, no decision logged", row(r).startswith("PREPARING") and prepared_count() == 1, row(r))
restart_coordinator()
check("recovery aborted the transaction (presumed abort)", bool(wait_until(lambda: row(r).startswith("COMPLETED/ABORTED"), 40)), row(r))
check("balances untouched, nothing prepared", (db_balance("db-a", "A0400"), db_balance("db-b", "A1400"), prepared_count()) == (1000, 1000, 0))

check("conservation of money across the whole experiment", total_money() == base_total, f"{total_money()}")
sys.exit(summary())
