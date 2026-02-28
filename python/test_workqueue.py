"""
test_workqueue.py  —  End-to-end tests for WorkQueue
-------------------------------------------------------
Make sure both services are running before executing:
  Terminal 1: uvicorn producer:app --port 8080 --reload
  Terminal 2: python worker.py

Run tests:
  python test_workqueue.py
"""

import time
import requests

PRODUCER_URL = "http://localhost:8080"
WORKER_URL   = "http://localhost:8081"

PASS = "\033[92m✔ PASS\033[0m"
FAIL = "\033[91m✘ FAIL\033[0m"


def check(label: str, condition: bool, info: str = "") -> bool:
    status = PASS if condition else FAIL
    print(f"  {status}  {label}" + (f"  →  {info}" if info else ""))
    return condition


# ──────────────────────────────────────────────────────────────────────────────
# Test 1 – Producer health check
# ──────────────────────────────────────────────────────────────────────────────
def test_health():
    print("\n[Test 1] Producer health check")
    try:
        r = requests.get(f"{PRODUCER_URL}/health", timeout=5)
        check("Status 200",       r.status_code == 200, str(r.status_code))
        check("Body has 'ok'",    r.json().get("status") == "ok", str(r.json()))
    except Exception as e:
        check("Producer reachable", False, str(e))


# ──────────────────────────────────────────────────────────────────────────────
# Test 2 – Enqueue a valid send_email task
# ──────────────────────────────────────────────────────────────────────────────
def test_enqueue_email():
    print("\n[Test 2] Enqueue valid send_email task")
    payload = {
        "type": "send_email",
        "payload": {"to": "user@example.com", "subject": "Test mail"},
        "retries": 2,
    }
    try:
        r = requests.post(f"{PRODUCER_URL}/enqueue", json=payload, timeout=5)
        check("Status 200",            r.status_code == 200, str(r.status_code))
        check("Task accepted message", "added to queue" in r.json().get("message", ""), str(r.json()))
    except Exception as e:
        check("Request succeeded", False, str(e))


# ──────────────────────────────────────────────────────────────────────────────
# Test 3 – Enqueue a generic task
# ──────────────────────────────────────────────────────────────────────────────
def test_enqueue_generic():
    print("\n[Test 3] Enqueue generic task (resize_image)")
    payload = {
        "type": "resize_image",
        "payload": {"url": "https://example.com/img.png", "width": 800},
        "retries": 1,
    }
    try:
        r = requests.post(f"{PRODUCER_URL}/enqueue", json=payload, timeout=5)
        check("Status 200", r.status_code == 200, str(r.status_code))
    except Exception as e:
        check("Request succeeded", False, str(e))


# ──────────────────────────────────────────────────────────────────────────────
# Test 4 – Validation: send_email missing required fields
# ──────────────────────────────────────────────────────────────────────────────
def test_invalid_email_task():
    print("\n[Test 4] Validation — send_email missing 'to' and 'subject'")
    payload = {"type": "send_email", "payload": {}}
    try:
        r = requests.post(f"{PRODUCER_URL}/enqueue", json=payload, timeout=5)
        check("Status 400", r.status_code == 400, str(r.status_code))
    except Exception as e:
        check("Request succeeded", False, str(e))


# ──────────────────────────────────────────────────────────────────────────────
# Test 5 – Validation: empty task type
# ──────────────────────────────────────────────────────────────────────────────
def test_empty_type():
    print("\n[Test 5] Validation — empty task type")
    payload = {"type": "   ", "payload": {}}
    try:
        r = requests.post(f"{PRODUCER_URL}/enqueue", json=payload, timeout=5)
        check("Status 422", r.status_code == 422, str(r.status_code))
    except Exception as e:
        check("Request succeeded", False, str(e))


# ──────────────────────────────────────────────────────────────────────────────
# Test 6 – Worker metrics endpoint
# ──────────────────────────────────────────────────────────────────────────────
def test_metrics():
    print("\n[Test 6] Worker metrics endpoint")
    # Give worker a moment to process the tasks enqueued above
    time.sleep(2)
    try:
        r = requests.get(f"{WORKER_URL}/metrics", timeout=5)
        data = r.json()
        check("Status 200",              r.status_code == 200, str(r.status_code))
        check("Has 'jobs_done' key",     "jobs_done"    in data, str(data))
        check("Has 'jobs_failed' key",   "jobs_failed"  in data, str(data))
        check("Has queue length key",    "total_jobs_in_queue" in data, str(data))
        print(f"         jobs_done={data.get('jobs_done')}  "
              f"jobs_failed={data.get('jobs_failed')}  "
              f"queue={data.get('total_jobs_in_queue')}")
    except Exception as e:
        check("Worker reachable", False, str(e))


# ──────────────────────────────────────────────────────────────────────────────
# Test 7 – Burst: enqueue 10 tasks rapidly
# ──────────────────────────────────────────────────────────────────────────────
def test_burst():
    print("\n[Test 7] Burst — enqueue 10 tasks quickly")
    success = 0
    for i in range(10):
        payload = {
            "type": "process_data",
            "payload": {"item_id": i, "value": i * 10},
            "retries": 0,
        }
        try:
            r = requests.post(f"{PRODUCER_URL}/enqueue", json=payload, timeout=5)
            if r.status_code == 200:
                success += 1
        except Exception:
            pass
    check(f"All 10 tasks enqueued", success == 10, f"{success}/10 succeeded")


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("  WorkQueue — End-to-End Test Suite")
    print("=" * 55)

    test_health()
    test_enqueue_email()
    test_enqueue_generic()
    test_invalid_email_task()
    test_empty_type()
    test_metrics()
    test_burst()

    print("\n" + "=" * 55)
    print("  Tests complete. Check worker terminal for processing logs.")
    print("=" * 55)
