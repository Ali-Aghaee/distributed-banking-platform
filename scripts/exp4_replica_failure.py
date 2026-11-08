from lib import *
from collections import Counter

title("Experiment 4 - Replica interruption under load")
wait_platform(); reset_balances()
stop_evt, log_ = threading.Event(), []
def load():
    while not stop_evt.is_set():
        a = f"A{random.randint(1, 1000):04d}"
        t = time.time()
        s, j, h = http("POST", f"{GW}/accounts/{a}/deposit", {"amount": "1", "request_id": rid()}, timeout=20)
        log_.append((t, s, h.get("x-served-by"), a))
ths = [threading.Thread(target=load, daemon=True) for _ in range(10)]
[t.start() for t in ths]
time.sleep(4)
say("docker stop bank-a1 while 10 clients are hammering shard A ...")
t_stop = time.time(); dc("stop", "bank-a1"); time.sleep(8)
reps = http("GET", GW + "/admin/replicas")[1]
a1 = [r for r in reps if r["name"] == "bank-a1"][0]
say(f"gateway view of bank-a1: healthy={a1['healthy']}")
stop_evt.set(); [t.join() for t in ths]
before = Counter(x[2] for x in log_ if x[0] < t_stop and x[1] == 200)
after = Counter(x[2] for x in log_ if x[0] > t_stop + 4 and x[1] == 200)
say(f"served before stop: {dict(before)}\nserved >4s after stop: {dict(after)}")
fails = [x for x in log_ if x[1] != 200]
check("gateway detected the replica as unhealthy", a1["healthy"] is False)
check("no new request was routed to the stopped replica", "bank-a1" not in after and "bank-a2" in after)
check("clients saw (almost) no failures thanks to retry on another replica", len(fails) <= 2, f"{len(fails)} failed of {len(log_)}")
good = sum(1 for x in log_ if x[1] == 200)
check("every acknowledged deposit is in the DB (no lost/duplicated updates)",
      Decimal(psql("db-a", "SELECT sum(balance) FROM accounts")) == 1000 * 1000 + good, f"acknowledged={good}")
say("restarting bank-a1 ...")
dc("start", "bank-a1")
check("restarted replica returns to rotation", bool(wait_until(lambda: [r for r in http('GET', GW + '/admin/replicas')[1] if r['name'] == 'bank-a1' and r['healthy']], 30)))
sys.exit(summary())
