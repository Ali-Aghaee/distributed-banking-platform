#!/usr/bin/env python3
"""Banking Client - talks ONLY to the Gateway (never sees shards or replicas). Stdlib only.

  python client/bank_client.py balance A0500
  python client/bank_client.py deposit A0500 100
  python client/bank_client.py withdraw A0500 50
  python client/bank_client.py transfer A0500 A1500 300
  python client/bank_client.py status <request_id | tx_id>
Writes carry a client-generated request_id; on network errors / 5xx the SAME request_id is
re-sent, which makes retries safe (exactly-once effect).
"""
import argparse, json, os, sys, time, urllib.error, urllib.request, uuid

GATEWAY = os.getenv("GATEWAY_URL", "http://localhost:8000")


def call(method, path, body=None):
    req = urllib.request.Request(GATEWAY + path, method=method, headers={"Content-Type": "application/json"},
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {"error": "HTTP_ERROR"}
    except Exception as e:
        return 0, {"error": "CONNECTION", "detail": str(e)}


def write(method, path, body, retries=6):
    for i in range(retries):
        status, out = call(method, path, body)
        if status in (200, 400, 404, 409, 422) and out.get("error") not in ("LOCK_TIMEOUT",):
            return status, out
        if status == 202:                       # cross-shard transfer still being finished by the coordinator
            for _ in range(20):
                time.sleep(1)
                s2, o2 = call("GET", f"/transactions/by-request/{body['request_id']}")
                if o2.get("client_status") not in ("PENDING", "UNKNOWN"):
                    return (200 if o2["client_status"] == "COMMITTED" else 409), o2
            return status, out
        time.sleep(0.3 * (i + 1))
    return status, out


def main():
    p = argparse.ArgumentParser(description="Distributed banking client")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("balance"); s.add_argument("account")
    for name in ("deposit", "withdraw"):
        s = sub.add_parser(name); s.add_argument("account"); s.add_argument("amount"); s.add_argument("--request-id")
    s = sub.add_parser("transfer"); s.add_argument("src"); s.add_argument("dst"); s.add_argument("amount"); s.add_argument("--request-id")
    s = sub.add_parser("status"); s.add_argument("id")
    a = p.parse_args()
    if a.cmd == "balance":
        st, out = call("GET", f"/accounts/{a.account}/balance")
    elif a.cmd in ("deposit", "withdraw"):
        st, out = write("POST", f"/accounts/{a.account}/{a.cmd}", {"amount": a.amount, "request_id": a.request_id or uuid.uuid4().hex})
    elif a.cmd == "transfer":
        st, out = write("POST", "/transfer", {"src": a.src, "dst": a.dst, "amount": a.amount, "request_id": a.request_id or uuid.uuid4().hex})
    else:
        st, out = call("GET", f"/transactions/{a.id}")
        if st == 404:
            st, out = call("GET", f"/transactions/by-request/{a.id}")
    print(st, json.dumps(out, indent=2))
    sys.exit(0 if st in (200, 202) else 1)


if __name__ == "__main__":
    main()
