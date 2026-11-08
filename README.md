# Distributed Banking Platform (Sharding, Replication, 2PC)

Python (FastAPI) + PostgreSQL 16 + Docker Compose. 2 shards x 2 replicas, shard-aware gateway,
Two-Phase-Commit coordinator with a durable decision log. Bonus I (live re-sharding) and Bonus II
(self-healing controller) included.

## Run
```bash
docker compose build
docker compose up -d            # db-coord, db-a, db-b, gateway, coordinator, bank-a1/a2/b1/b2
python client/bank_client.py balance A0500
python scripts/run_all.py       # experiments 1-7
python scripts/exp8_bonus_live_resharding.py   # Bonus I  (starts shard C at runtime)
python scripts/exp9_bonus_self_healing.py      # Bonus II (starts the healer)
docker compose down -v          # full reset (re-seeds the databases)
```

## Layout
| path | role |
|---|---|
| `app/gateway.py` | shard selection (Shard Map), replica selection (least-connections / round-robin), health checks, retries |
| `app/bank_server.py` | stateless shard replica: local ops, same-shard transfer, 2PC participant, epoch fencing |
| `app/coordinator.py` | 2PC coordinator, durable `tx_log`, recovery loop, fault injection |
| `app/migrator.py` | Bonus I: snapshot + change capture + fencing + epoch cutover state machine |
| `app/healer.py` | Bonus II: self-healing controller (Docker API, no orchestrator, no restart policies) |
| `db/` | schemas / seeds / shard map metadata |
| `client/bank_client.py` | banking client (gateway only) |
| `scripts/exp*.py` | the 7 required experiments + 2 bonus experiments |

## Key design points
* Shard Map (ranges + epoch) is stored in the metadata DB; A0001-A1000 -> shard A, A1001-A2000 -> shard B.
* Replica consistency: one PostgreSQL tx per operation with row locks (`FOR UPDATE`, sorted order); journal table gives exactly-once.
* 2PC uses PostgreSQL `PREPARE TRANSACTION`; decisions are logged before they are sent; participants are idempotent.
* Fault injection (`POST :8100/admin/fault`, `:8200/admin/fault`, `:8300/fault`) is used by the experiments to crash components at precise points.
