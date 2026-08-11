"""Ban traffic vao inference-api de dashboard serving co so lieu.

Chay tu may host (khong can vao container):
    python scripts/load_test.py --rps 20 --duration 120

Chi dung thu vien chuan de khoi phai cai them gi.
"""
from __future__ import annotations

import argparse
import json
import random
import statistics
import threading
import time
import urllib.error
import urllib.request

API = "http://localhost:8000"


def call_decide(user_id: str, timeout: float = 5.0) -> tuple[bool, float, dict]:
    body = json.dumps({"user_id": user_id}).encode()
    req = urllib.request.Request(
        f"{API}/decide", data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read())
        return True, (time.perf_counter() - started) * 1000, payload
    except urllib.error.HTTPError as exc:
        return False, (time.perf_counter() - started) * 1000, {"error": exc.read().decode()[:200]}
    except Exception as exc:
        return False, (time.perf_counter() - started) * 1000, {"error": str(exc)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rps", type=float, default=20)
    parser.add_argument("--duration", type=int, default=60, help="giay")
    parser.add_argument("--users", type=int, default=20000)
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()

    latencies: list[float] = []
    decisions: dict[str, int] = {}
    errors = 0
    lock = threading.Lock()
    stop_at = time.time() + args.duration
    interval = args.threads / max(args.rps, 0.1)

    def worker() -> None:
        nonlocal errors
        while time.time() < stop_at:
            user_id = f"U{random.randint(0, args.users - 1):07d}"
            ok, ms, payload = call_decide(user_id)
            with lock:
                latencies.append(ms)
                if ok:
                    d = payload.get("decision", "?")
                    decisions[d] = decisions.get(d, 0) + 1
                else:
                    errors += 1
            time.sleep(interval)

    print(f"Ban {args.rps} rps trong {args.duration}s toi {API}/decide ...")
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(args.threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if not latencies:
        print("Khong goi duoc request nao - API da chay chua?")
        return

    latencies.sort()
    p = lambda q: latencies[min(int(len(latencies) * q), len(latencies) - 1)]  # noqa: E731
    print(f"""
    ----------------------------------------
    requests   : {len(latencies)}
    errors     : {errors}
    p50        : {p(0.50):.2f} ms
    p95        : {p(0.95):.2f} ms
    p99        : {p(0.99):.2f} ms   (SLA < 100ms)
    max        : {latencies[-1]:.2f} ms
    mean       : {statistics.mean(latencies):.2f} ms
    decisions  : {decisions}
    ----------------------------------------
    Xem chi tiet o Grafana -> dashboard "00 · End-to-End Pipeline Overview"
    """)


if __name__ == "__main__":
    main()
