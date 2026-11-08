from lib import *

title("Experiment 9 (Bonus II) - Autonomous self-healing under load and injected crashes")
wait_platform()
dc("--profile", "healing", "up", "-d", "healer")

def start_healer():
    subprocess.run(["docker", "start", "healer"], capture_output=True)
    dc("--profile", "healing", "start", "healer")
    return http("GET", HEAL + "/health", timeout=2)[0] == 200

assert wait_until(start_healer, 60, 1.0), "healer not up"
shards = psql("db-coord", "SELECT shard||':'||desired_replicas FROM shards WHERE enabled ORDER BY shard").split()
desired = {x.split(":")[0]: int(x.split(":")[1]) for x in shards}
say(f"desired replicas (from metadata DB): {desired}")

def containers(shard, only_running=False):
    args = ["docker", "ps", "-q" if only_running else "-aq", "--filter", "label=bank.managed=true", "--filter", f"label=bank.shard={shard}"]
    if only_running:
        args += ["--filter", "status=running"]
    return subprocess.run(args, capture_output=True, text=True).stdout.split()

def names(shard):
    r = subprocess.run(["docker", "ps", "-a", "--filter", "label=bank.managed=true", "--filter", f"label=bank.shard={shard}", "--format", "{{.Names}}:{{.State}}"], capture_output=True, text=True)
    return r.stdout.split()

def started(name):
    return subprocess.run(["docker", "inspect", "-f", "{{.State.StartedAt}}", name], capture_output=True, text=True).stdout.strip()

def healthy_count(shard):
    return sum(1 for r in http("GET", GW + "/admin/replicas")[1] if r["shard"] == shard and r["healthy"])

def restored(shard):
    return len(containers(shard, True)) == desired[shard] and healthy_count(shard) >= desired[shard]

assert wait_until(lambda: all(restored(s) for s in desired), 90, 1.0), "initial replicas not restored"
reset_balances()

say("\nStep 1: start concurrent banking load across every shard")
wl = Workload([(1, 2000)], workers=8, cross_partner_ranges=[(1, 2000)])
wl.start(); time.sleep(4)
bystander = [n.split(":")[0] for n in names("B") if ":running" in n][-1]
bystander_started = started(bystander)

say("\nStep 2: kill one replica of shard A (docker kill) -> controller must replace it")
victim = [n.split(":")[0] for n in names("A") if ":running" in n][0]
t0 = time.time()
subprocess.run(["docker", "kill", victim], capture_output=True)
ok = wait_until(lambda: restored("A") and victim not in " ".join(names("A")), 90, 1)
check("shard A back to 2 healthy replicas automatically", bool(ok), f"{time.time() - t0:.1f}s; containers={names('A')}")
check("replacement has a new name and the dead container is gone", victim not in " ".join(names("A")) and len(names("A")) == 2)
check("healthy replicas of other shards were NOT restarted", started(bystander) == bystander_started)

say("\nStep 3: injected controller crash between 'create container' and 'start' (shard B)")
victim_b = [n.split(":")[0] for n in names("B") if ":running" in n and n.split(":")[0] != bystander][0]
http("POST", HEAL + "/fault", {"point": "after_create"})
subprocess.run(["docker", "kill", victim_b], capture_output=True)
died = wait_until(lambda: http("GET", HEAL + "/health", timeout=2)[0] != 200, 60, 0.5)
check("controller crashed mid-recovery", bool(died))
say(f"   shard B containers while controller is down: {names('B')}")
check("a created-but-not-started replacement exists (the window we injected)", any(":created" in n for n in names("B")))
assert wait_until(start_healer, 40, 1.0)
ok = wait_until(lambda: restored("B"), 90, 1)
check("restarted controller adopted the container instead of creating a duplicate", bool(ok) and len(names("B")) == desired["B"], str(names("B")))

say("\nStep 4: kill ALL replicas of shard A at once (total shard outage) during load")
for c in containers("A", True):
    subprocess.run(["docker", "kill", c], capture_output=True)
ok = wait_until(lambda: restored("A"), 120, 1)
check("controller rebuilt the whole replica set of shard A", bool(ok), str(names("A")))
check("never more than the configured number of replicas (no duplicates)", all(len(names(s)) == desired[s] for s in desired), str({s: names(s) for s in desired}))

say("\nStep 5: let the load finish and verify transactional correctness")
time.sleep(5); wl.stop()
touched = sorted(wl.delta)
got = run_parallel(lambda a: (a, balance(a)[0]), touched, 20)
bad = [(a, str(g), str(1000 + wl.delta[a])) for a, g in got if g != 1000 + wl.delta[a]]
say(f"workload: {wl.counts}")
check(f"all {len(touched)} touched accounts have exactly the acknowledged effects", not bad, str(bad[:3]))
check("no operation with unknown outcome", wl.counts["unknown"] == 0)
check("money conserved", total_money() == Decimal(2_000_000) + wl.net_external, str(total_money()))
check("no prepared / unresolved transactions", prepared_count() == 0 and psql("db-coord", "SELECT count(*) FROM tx_log WHERE status<>'COMPLETED'") == "0")
say("\ncontroller history:\n" + "\n".join(f"  {h['ts']} {h['event']}" for h in http("GET", HEAL + "/status")[1]["history"]))
sys.exit(summary())