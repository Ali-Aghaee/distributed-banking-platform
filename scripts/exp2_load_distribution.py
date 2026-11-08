from lib import *
from collections import Counter

title("Experiment 2 - Replica load distribution inside a shard")
wait_platform()
N = 300
def one(i):
    s, j, h = (http("GET", f"{GW}/accounts/A{random.randint(1, 1000):04d}/balance") if i % 2 else
               http("POST", f"{GW}/accounts/A{random.randint(1, 1000):04d}/deposit", {"amount": "1", "request_id": rid()}))
    return s, h.get("x-served-by")
res = run_parallel(one, range(N), 30)
cnt = Counter(r[1] for r in res if r[0] == 200)
say(f"{N} requests to Shard A accounts, served by: {dict(cnt)}")
check("all requests succeeded", all(r[0] == 200 for r in res))
check("both replicas of shard A served traffic", set(cnt) == {"bank-a1", "bank-a2"})
lo, hi = min(cnt.values()), max(cnt.values())
check("load is reasonably balanced", lo / hi > 0.3, f"min/max={lo}/{hi}")
for rep in ("bank-a1", "bank-a2"):
    out = subprocess.run(["docker", "logs", "--tail", "3", rep], capture_output=True, text=True)
    say(f"--- docker logs {rep} (last lines; the logger name identifies the replica) ---\n" + (out.stdout + out.stderr).strip())
say("\nGateway routing log sample:")
out = subprocess.run(["docker", "logs", "--tail", "400", "gateway"], capture_output=True, text=True)
say("\n".join([l for l in (out.stdout + out.stderr).splitlines() if "route account" in l][-4:]))
sys.exit(summary())
