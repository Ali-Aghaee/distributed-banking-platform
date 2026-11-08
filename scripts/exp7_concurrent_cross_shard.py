from lib import *

title("Experiment 7 - Concurrent cross-shard (and local) transfers on overlapping accounts")
wait_platform(); reset_balances()
accts = ["A0001", "A0002", "A0003", "A0004", "A1001", "A1002", "A1003", "A1004"]
base_total = total_money()
random.seed(7)
jobs = []
for _ in range(150):
    a, b = random.sample(accts, 2)
    jobs.append((a, b, random.randint(1, 30)))
res = run_parallel(lambda j: (j, transfer(j[0], j[1], j[2], retries=10)), jobs, 30)
exp = {a: Decimal(1000) for a in accts}
counts = {"COMMITTED": 0, "ABORTED": 0, "UNKNOWN": 0}
for (a, b, amt), (st, body) in res:
    counts[st] += 1
    if st == "COMMITTED":
        exp[a] -= amt; exp[b] += amt
cross = sum(1 for (a, b, _), _ in res if (a < "A1001") != (b < "A1001"))
say(f"{len(jobs)} transfers ({cross} cross-shard, {len(jobs) - cross} same-shard): {counts}")
check("no transfer left with an unknown outcome", counts["UNKNOWN"] == 0)
check("(nearly) all transfers committed", counts["COMMITTED"] >= 0.95 * len(jobs))
for a in accts:
    actual = balance(a)[0]
    check(f"{a} balance matches the sum of committed transfers", actual == exp[a], f"{actual} == {exp[a]}")
check("money conserved (total of all shards unchanged)", total_money() == base_total, f"{total_money()}")
check("no negative balance", psql("db-a", "SELECT count(*) FROM accounts WHERE balance<0") == "0" and psql("db-b", "SELECT count(*) FROM accounts WHERE balance<0") == "0")
check("no unresolved transaction in the coordinator log", psql("db-coord", "SELECT count(*) FROM tx_log WHERE status<>'COMPLETED'") == "0")
check("no prepared transaction left in any shard", prepared_count() == 0)
say("tx_log summary: " + psql("db-coord", "SELECT status||'/'||coalesce(outcome,'-')||'/'||coalesce(reason,'-')||': '||count(*) FROM tx_log GROUP BY status, outcome, reason ORDER BY 1").replace("\n", " | "))
sys.exit(summary())