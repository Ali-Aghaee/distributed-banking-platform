from lib import *
from collections import Counter

title("Experiment 3 - Concurrent operations through different replicas of one shard")
wait_platform(); reset_balances()
acct = "A0010"
say("Phase 1: 100 deposits of 1 and 50 withdrawals of 1 on the same account, 40 threads")
jobs = [("deposit", 1)] * 100 + [("withdraw", 1)] * 50
random.shuffle(jobs)
res = run_parallel(lambda j: op(j[0], acct, j[1]), jobs, 40)
used = Counter(r[2].get("x-served-by") for r in res)
check("all 150 operations succeeded", all(r[0] == 200 for r in res))
check("operations were handled by both replicas", len(used) == 2, str(dict(used)))
check("final balance = 1000 + 100 - 50 = 1050 (no lost update)", balance(acct)[0] == 1050, f"balance={balance(acct)[0]}")
check("DB agrees", db_balance("db-a", acct) == 1050)
say("\nPhase 2: balance 100, 30 concurrent withdrawals of 10 -> exactly 10 must succeed, never negative")
psql("db-a", "UPDATE accounts SET balance=100.00, version=version+1 WHERE id='A0011'")
res = run_parallel(lambda i: op("withdraw", "A0011", 10), range(30), 30)
ok = sum(1 for r in res if r[0] == 200)
check("exactly 10 withdrawals succeeded, 20 rejected (INSUFFICIENT_FUNDS)", ok == 10 and sum(1 for r in res if r[0] == 422) == 20, f"ok={ok}")
check("final balance is 0", db_balance("db-a", "A0011") == 0)
say("\nPhase 3: idempotency - the SAME request_id sent 10 times concurrently is applied once")
r0, before = rid(), db_balance("db-a", "A0012")
res = run_parallel(lambda i: op("deposit", "A0012", 25, request_id=r0), range(10), 10)
check("applied exactly once", db_balance("db-a", "A0012") == before + 25, f"{before} -> {db_balance('db-a', 'A0012')}")
sys.exit(summary())
