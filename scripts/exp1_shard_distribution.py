from lib import *

title("Experiment 1 - Shard distribution and shard-aware routing")
wait_platform()
s, j, _ = http("GET", GW + "/admin/shard-map")
say(f"Shard Map (from metadata DB): {json.dumps(j)}")
samples = {"db-a": ["A0001", "A0500", "A1000"], "db-b": ["A1001", "A1500", "A2000"]}
for db, other in (("db-a", "db-b"), ("db-b", "db-a")):
    n = psql(db, "SELECT count(*)||' accounts, '||min(id)||'..'||max(id) FROM accounts")
    say(f"  {db}: {n}")
    for a in samples[db]:
        b, body, h = balance(a)
        check(f"{a} stored in {db} only", db_balance(db, a) is not None and db_balance(other, a) is None)
        check(f"{a} readable via gateway", b == 1000 or b is not None, f"balance={b}, served_by={h.get('x-served-by')}, shard={body.get('shard')}")
        check(f"{a} routed to the owning shard", body.get("shard") == ("A" if db == "db-a" else "B"))
say("\nDeposit 10 through the gateway and verify ONLY the owning database changed:")
for a, owner, other in (("A0777", "db-a", "db-b"), ("A1777", "db-b", "db-a")):
    before = db_balance(owner, a)
    s, j, _ = op("deposit", a, 10)
    check(f"deposit {a}", s == 200 and db_balance(owner, a) == before + 10 and db_balance(other, a) is None, f"{before} -> {db_balance(owner, a)}")
sys.exit(summary())
