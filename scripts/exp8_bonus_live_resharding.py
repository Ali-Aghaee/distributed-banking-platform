from lib import *

title("Experiment 8 (Bonus I) - Live re-sharding: add Shard C and move A1501-A2000 from B to C under load")
wait_platform()
say("Step 1: start Shard C (own DB + 2 replicas) and the migrator at RUNTIME ...")
dc("stop", "migrator")
dc("--profile", "shard-c", "up", "-d", "db-c", "bank-c1", "bank-c2")
assert wait_until(lambda: psql("db-c", "SELECT 1") == "1", 90), "db-c not up"
psql("db-coord", "TRUNCATE migration, tx_log, tx_events RESTART IDENTITY; DELETE FROM shard_map; INSERT INTO shard_map VALUES (1, 1000, 'A', 1), (1001, 2000, 'B', 1); UPDATE map_epoch SET epoch=1; UPDATE shards SET enabled=FALSE WHERE shard='C'")
psql("db-a", "TRUNCATE accounts, chunks, journal, tx_state, changelog RESTART IDENTITY; INSERT INTO accounts(id, balance) SELECT 'A' || lpad(g::text, 4, '0'), 1000.00 FROM generate_series(1, 1000) g; INSERT INTO chunks SELECT c, c*100+1, c*100+100, 'ACTIVE', 1 FROM generate_series(0, 9) c; TRUNCATE changelog RESTART IDENTITY")
psql("db-b", "TRUNCATE accounts, chunks, journal, tx_state, changelog RESTART IDENTITY; INSERT INTO accounts(id, balance) SELECT 'A' || lpad(g::text, 4, '0'), 1000.00 FROM generate_series(1001, 2000) g; INSERT INTO chunks SELECT c, c*100+1, c*100+100, 'ACTIVE', 1 FROM generate_series(10, 19) c; TRUNCATE changelog RESTART IDENTITY")
psql("db-c", "TRUNCATE accounts, chunks, journal, tx_state, changelog RESTART IDENTITY")
dc("--profile", "shard-c", "up", "-d", "migrator")
assert wait_platform(("A", "B", "C")), "shard C replicas not healthy"
assert wait_until(lambda: http("GET", MIG + "/health", timeout=3)[0] == 200, 60), "migrator not up"
BASE = Decimal(2_000_000)
net_total = Decimal(0)
from collections import defaultdict
exp_bal = defaultdict(lambda: Decimal(1000))      # expected balance of every account touched so far

def phase_of(mid):
    return psql("db-coord", f"SELECT phase FROM migration WHERE id={mid}")

def verify_workload(wl, label):
    global net_total
    touched = sorted(wl.delta)
    got = run_parallel(lambda a: (a, balance(a)[0]), touched, 20)
    bad = []
    for a, g in got:
        exp_bal[a] += wl.delta[a]
        if g != exp_bal[a]:
            bad.append((a, str(g), str(exp_bal[a])))
    net_total += wl.net_external
    check(f"{label}: {len(touched)} touched accounts have exactly the acknowledged effects (no loss, no duplicate)", not bad, str(bad[:3]))
    check(f"{label}: no operation left with an unknown outcome", wl.counts["unknown"] == 0, str(wl.counts))
    check(f"{label}: money conserved (2,000,000 + net external deposits)", total_money() == BASE + net_total, f"{total_money()} vs {BASE + net_total}")
    check(f"{label}: no prepared txs / unresolved coordinator txs", prepared_count() == 0 and psql("db-coord", "SELECT count(*) FROM tx_log WHERE status<>'COMPLETED'") == "0")

say("\nStep 2: background workload on A1501-A2000 (deposits, withdrawals, same-shard and cross-shard transfers)")
s, j, _ = op("deposit", "A1650", 7)
dup_rid = rid(); s, j, _ = op("deposit", "A1651", 9, request_id=dup_rid); exp_bal["A1651"] += 9; exp_bal["A1650"] += 7; net_total += 16
wl = Workload([(1501, 2000)], workers=6, cross_partner_ranges=[(1, 1000), (1001, 1500), (1501, 2000)])
wl.start(); time.sleep(3)
s, m, _ = http("POST", MIG + "/migrations", {"src": "B", "dst": "C", "lo": 1501, "hi": 2000})
check("migration accepted", s == 200, str(m.get("new_epoch")))
mid, seen = m["id"], []
def done():
    p = phase_of(mid)
    if not seen or seen[-1] != p:
        seen.append(p); say(f"   phase -> {p}")
    return p in ("DONE", "FAILED")
wait_until(done, 240, 0.3)
time.sleep(3); wl.stop()
check("migration finished DONE", phase_of(mid) == "DONE", " -> ".join(seen))
check("workload kept running during the migration", wl.counts["ok"] > 50, str(wl.counts))
verify_workload(wl, "after migration")
say("shard map now: " + json.dumps(http("GET", GW + "/admin/shard-map")[1]["ranges"]))
check("A1501-A2000 owned by C at epoch 2", any(r["shard"] == "C" and r["lo"] == 1501 and r["hi"] == 2000 and r["epoch"] == 2 for r in http("GET", GW + "/admin/shard-map")[1]["ranges"]))
check("Shard C holds 500 accounts, Shard B holds 500 (old data removed after verification)",
      (psql("db-c", "SELECT count(*) FROM accounts"), psql("db-b", "SELECT count(*) FROM accounts")) == ("500", "500"))
say("\nStep 3: stale-route fencing - write directly to the shards with the OLD epoch")
before = balance("A1600")[0]
s1, j1, _ = http("POST", GW + "/debug/direct", {"shard": "B", "account": "A1600", "amount": "5", "epoch": 1})
s2, j2, _ = http("POST", GW + "/debug/direct", {"shard": "C", "account": "A1600", "amount": "5", "epoch": 1})
s3, j3, _ = http("POST", GW + "/debug/direct", {"shard": "B", "account": "A1600", "amount": "5", "epoch": 2})
check("old owner B rejects stale write (epoch 1)", s1 == 409 and j1["error"] == "FENCED", j1.get("detail"))
check("old owner B rejects even a current-epoch write (it no longer owns the range)", s3 == 409 and j3["error"] == "FENCED")
check("new owner C rejects stale epoch 1", s2 == 409 and j2["error"] == "STALE_EPOCH", j2.get("detail"))
check("balance unchanged by all stale writes", balance("A1600")[0] == before)
s, j, _ = http("POST", f"{GW}/accounts/A1651/deposit", {"amount": "9", "request_id": dup_rid})
check("duplicate of a pre-migration request is recognised on the new shard (journal migrated, exactly-once)", s == 200 and j.get("replayed") is True)

say("\nStep 4: crash-consistent state machine - kill the migrator in each phase, restart, converge")
plan = [("SNAPSHOT", 1101, 1200), ("DELTA", 1201, 1300), ("FENCING", 1301, 1400), ("MAP_ACTIVATED", 1401, 1500)]
for point, lo, hi in plan:
    say(f"\n--- kill migrator at {point}: moving A{lo:04d}-A{hi:04d} B -> C under load ---")
    wl = Workload([(lo, hi)], workers=4, cross_partner_ranges=[(1, 1000), (lo, hi)])
    wl.start(); time.sleep(2)
    http("POST", MIG + "/admin/fault", {"phase": point})
    s, m, _ = http("POST", MIG + "/migrations", {"src": "B", "dst": "C", "lo": lo, "hi": hi})
    mid = m["id"]
    dead = wait_until(lambda: http("GET", MIG + "/health", timeout=2)[0] != 200, 150, 0.3)
    ph = phase_of(mid)
    check(f"migrator was killed abruptly at {point}; persisted phase = {ph}", bool(dead))
    if point == "MAP_ACTIVATED":
        mp = psql("db-coord", f"SELECT shard FROM shard_map WHERE lo<={lo} AND hi>={hi}")
        check("crashed AFTER map activation, BEFORE cleanup (old data still on B)", mp == "C" and psql("db-b", f"SELECT count(*) FROM accounts WHERE id BETWEEN 'A{lo:04d}' AND 'A{hi:04d}'") == "100")
    dc("start", "migrator")
    wait_until(lambda: phase_of(mid) in ("DONE", "FAILED"), 180, 0.5)
    time.sleep(2); wl.stop()
    check(f"recovered automatically and converged to DONE (no manual edits)", phase_of(mid) == "DONE", phase_of(mid))
    check("old data removed only after completion; chunks permanently MOVED",
          psql("db-b", f"SELECT count(*) FROM accounts WHERE id BETWEEN 'A{lo:04d}' AND 'A{hi:04d}'") == "0"
          and psql("db-b", f"SELECT string_agg(DISTINCT status, ',') FROM chunks WHERE chunk_id BETWEEN {(lo-1)//100} AND {(hi-1)//100}") == "MOVED"
          and psql("db-c", f"SELECT count(*) FROM accounts WHERE id BETWEEN 'A{lo:04d}' AND 'A{hi:04d}'") == "100")
    verify_workload(wl, f"after crash@{point}")
say("\nfinal shard map: " + json.dumps(http("GET", GW + "/admin/shard-map")[1]["ranges"]))
sys.exit(summary())