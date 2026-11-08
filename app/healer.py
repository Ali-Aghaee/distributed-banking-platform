"""Self-Healing Controller (Bonus II) - custom, no Kubernetes/Swarm/restart policies.

Level-triggered reconciliation loop. The ONLY source of truth is the Docker daemon (labels
bank.managed=true, bank.shard=X), so the controller keeps no state that could be lost:
  * desired replicas per shard come from the `shards` table (enabled shards),
  * failed replica (exited / dead / unresponsive for FAIL_THRESHOLD checks) -> deregister + remove,
  * missing replicas -> create (+ start, wait healthy, register with the gateway),
  * a container that exists but was never started (controller crashed between create and start)
    is ADOPTED and started, not duplicated.
Because every pass recomputes "desired - actual" from Docker itself, a crash at any point cannot
create duplicate replicas. Healthy replicas are never touched.
"""
import logging, os, re, threading, time, uuid
from datetime import datetime, timezone

import docker, httpx, uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from .common import env, install_common, make_pool, setup_logging

setup_logging()
log = logging.getLogger("healer")
app = FastAPI(title="healer")
install_common(app, "healer")
META_URL, GATEWAY = env("META_URL"), env("GATEWAY_URL", "http://gateway:8000")
IMAGE, NETWORK = env("IMAGE", "banking-platform:latest"), env("NETWORK", "bank-net")
INTERVAL, GRACE, FAIL_THRESHOLD = 2.0, 15.0, 3
dk = docker.from_env()
meta = None
fails, history, fault = {}, [], {"point": None}


def note(msg):
    log.info(msg)
    history.append({"ts": time.strftime("%H:%M:%S"), "event": msg})
    del history[:-200]


def crash_point(name):
    if fault["point"] == name:
        log.error("FAULT INJECTION: healer crashing at '%s'", name)
        logging.shutdown()
        os._exit(1)


def healthy(name):
    try:
        return httpx.get(f"http://{name}:8000/health", timeout=1.5).status_code == 200
    except Exception:
        return False


def age_seconds(c):
    s = c.attrs.get("State", {}).get("StartedAt", "")
    try:
        s = re.sub(r"(\.\d{1,6})\d*Z$", r"\1+00:00", s) if "." in s else s.replace("Z", "+00:00")
        return (datetime.now(timezone.utc) - datetime.fromisoformat(s)).total_seconds()
    except Exception:
        return 0.0


def gateway(path, name, shard="", url=""):
    try:
        httpx.post(GATEWAY + path, json={"name": name, "shard": shard, "url": url}, timeout=3)
    except Exception as e:
        log.warning("gateway %s failed: %s", path, e)


def remove(c, why):
    note(f"replica {c.name} FAILED ({why}) -> deregister + remove")
    gateway("/admin/deregister", c.name)
    try:
        c.remove(force=True)
    except Exception as e:
        log.warning("remove %s: %s", c.name, e)
    fails.pop(c.name, None)


def spawn(shard, db_url):
    name = f"bank-{shard.lower()}-{uuid.uuid4().hex[:6]}"
    c = dk.containers.create(IMAGE, command=["python", "-m", "app.bank_server"], name=name, hostname=name,
                             network=NETWORK, labels={"bank.managed": "true", "bank.shard": shard, "bank.created_by": "healer"},
                             environment={"REPLICA_NAME": name, "SHARD": shard, "DB_URL": db_url, "GATEWAY_URL": GATEWAY})
    note(f"replacement container {name} created for shard {shard}")
    crash_point("after_create")                  # crash window: created but not started -> adopted next pass
    c.start()
    for _ in range(30):
        if healthy(name):
            gateway("/admin/register", name, shard, f"http://{name}:8000")
            note(f"replacement {name} healthy and registered with the gateway")
            return
        time.sleep(1)
    note(f"replacement {name} not healthy yet (will be re-evaluated)")


def reconcile():
    with meta.connection() as c:
        shards = c.execute("SELECT shard, db_url, desired_replicas FROM shards WHERE enabled").fetchall()
    for s in shards:
        alive = 0
        for c in dk.containers.list(all=True, filters={"label": ["bank.managed=true", f"bank.shard={s['shard']}"]}):
            c.reload()
            if c.status == "created":
                c.start()
                note(f"adopted never-started container {c.name} and started it")
                alive += 1
            elif c.status == "running":
                if healthy(c.name):
                    fails[c.name] = 0
                    alive += 1
                else:
                    fails[c.name] = fails.get(c.name, 0) + 1
                    if age_seconds(c) < GRACE or fails[c.name] < FAIL_THRESHOLD:
                        alive += 1                # still booting / transient blip
                    else:
                        remove(c, "unresponsive")
            else:
                remove(c, f"state={c.status}")
        for _ in range(max(0, s["desired_replicas"] - alive)):
            spawn(s["shard"], s["db_url"])


def loop():
    while True:
        try:
            reconcile()
        except Exception as e:
            log.error("reconcile error: %s", e)
        time.sleep(INTERVAL)


@app.get("/status")
def status():
    out = {}
    for c in dk.containers.list(all=True, filters={"label": "bank.managed=true"}):
        out.setdefault(c.labels.get("bank.shard"), []).append({"name": c.name, "state": c.status})
    return {"replicas": out, "history": history[-30:]}


class FaultReq(BaseModel):
    point: str | None = None


@app.post("/fault")
def set_fault(f: FaultReq):
    """Fault injection: after_create"""
    fault["point"] = f.point
    return {"armed": f.point}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.on_event("startup")
def startup():
    global meta
    meta = make_pool(META_URL, 5)
    note("healer started: reconciling from Docker state")
    threading.Thread(target=loop, daemon=True).start()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False, log_level="warning")
