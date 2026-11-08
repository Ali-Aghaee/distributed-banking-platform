from lib import *

title("Experiment 5 - Same-shard vs cross-shard transfers")
wait_platform(); reset_balances()
say("--- same-shard transfer A0001 -> A0002 (300): one local DB transaction, NO coordinator ---")
s, j, h = http("POST", GW + "/transfer", {"src": "A0001", "dst": "A0002", "amount": "300", "request_id": rid()})
check("local transfer ok", s == 200 and "tx_id" not in j, f"served_by={h.get('x-served-by')}")
check("balances 700 / 1300", (db_balance("db-a", "A0001"), db_balance("db-a", "A0002")) == (700, 1300))
say("--- cross-shard transfer A0500 (Shard A, 1000) -> A1500 (Shard B, 500) amount 300 (spec example) ---")
psql("db-b", "UPDATE accounts SET balance=500.00, version=version+1 WHERE id='A1500'")
r = rid()
s, j, h = http("POST", GW + "/transfer", {"src": "A0500", "dst": "A1500", "amount": "300", "request_id": r})
check("cross-shard transfer committed through 2PC", s == 200 and j["client_status"] == "COMMITTED", f"tx={j.get('tx_id')}")
check("balances 700 / 800", (db_balance("db-a", "A0500"), db_balance("db-b", "A1500")) == (700, 800))
st = http("GET", f"{GW}/transactions/{j['tx_id']}")[1]
say("2PC audit trail: " + " -> ".join(e["status"] for e in st["events"]))
check("trail contains the full protocol", [e["status"] for e in st["events"]] == ["INITIATED", "PREPARING", "PREPARED", "COMMIT_DECIDED", "COMPLETED"])
check("no prepared transaction left", prepared_count() == 0)
say("--- duplicate delivery: same request_id again must NOT move money twice ---")
s2, j2, _ = http("POST", GW + "/transfer", {"src": "A0500", "dst": "A1500", "amount": "300", "request_id": r})
check("idempotent replay", s2 == 200 and j2["tx_id"] == j["tx_id"] and db_balance("db-a", "A0500") == 700)
say("--- Scenario A: one shard votes YES, the other rejects (insufficient funds on the 2nd participant) ---")
s, j, _ = http("POST", GW + "/transfer", {"src": "A1501", "dst": "A0501", "amount": "5000", "request_id": rid()})
parts = {p["account"]: p["vote"] for p in j["participants"]}
check("aborted, funds untouched", j["client_status"] == "ABORTED" and s == 422, f"reason={j['reason']} votes={parts}")
check("A0501 had voted YES and was safely rolled back (nothing prepared, balance intact)",
      parts.get("A0501") == "YES" and prepared_count() == 0 and db_balance("db-a", "A0501") == 1000 and db_balance("db-b", "A1501") == 1000)
say("""
Explanation: a same-shard transfer touches one database, so a single local ACID transaction is atomic.
A cross-shard transfer touches two independent databases; without 2PC a crash between the two updates
could debit one side only, so PREPARE/COMMIT with a durable coordinator decision is required.""")
sys.exit(summary())
