"""
Worker Service — worker.py
- Uses concurrent.futures.ThreadPoolExecutor for concurrency
- Polls Redis with BRPOP (blocking pop from right)
- Retries failed tasks up to the configured limit
- Exposes GET /health and GET /metrics via FastAPI
- Worker threads start via FastAPI lifespan (works with uvicorn)
"""

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

import redis
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI

from logger import log_failure, log_success
from processor import process_task

load_dotenv()

REDIS_URL   = os.getenv("REDIS_URL", "redis://localhost:6379")
PORT        = int(os.getenv("PORT_WORKER", 8081))
NUM_WORKERS = 3
QUEUE_KEY   = "task_queue"

rdb = redis.from_url(REDIS_URL, decode_responses=True)

# ── Shared metrics (thread-safe) ──────────────────────────────────────────
_lock       = threading.Lock()
jobs_done   = 0
jobs_failed = 0

# ── Lifespan: starts worker threads when uvicorn launches ────────────────
_executor = ThreadPoolExecutor(max_workers=NUM_WORKERS)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start worker threads on startup."""
    for i in range(NUM_WORKERS):
        _executor.submit(run_worker, i)
    print(f"[WorkQueue] {NUM_WORKERS} workers running")
    yield
    _executor.shutdown(wait=False)

app = FastAPI(title="WorkQueue Worker", lifespan=lifespan)


@app.get("/health")
def health():
    """Keep-alive endpoint — also used by Render health checks."""
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    with _lock:
        return {
            "total_jobs_in_queue": rdb.llen(QUEUE_KEY),
            "jobs_done":           jobs_done,
            "jobs_failed":         jobs_failed,
        }


# ── Worker logic ──────────────────────────────────────────────────────────
def run_worker(worker_id: int) -> None:
    """Continuously pop and process one task at a time."""
    print(f"[Worker-{worker_id}] Started")
    while True:
        result = rdb.brpop(QUEUE_KEY, timeout=0)   # blocks until a task arrives
        if result is None:
            continue

        _, raw = result
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"[Worker-{worker_id}] Bad JSON: {e}")
            continue

        task_type = data.get("type", "")
        payload   = data.get("payload", {})
        retries   = data.get("retries", 0)

        try:
            process_task(task_type, payload)
            with _lock:
                global jobs_done
                jobs_done += 1
            log_success(task_type, payload)
            print(f"[Worker-{worker_id}] ✔ '{task_type}' done")

        except Exception as err:
            with _lock:
                global jobs_failed
                jobs_failed += 1
            log_failure(task_type, payload, err)
            print(f"[Worker-{worker_id}] ✘ '{task_type}' failed — {err}")

            # Retry: re-queue with decremented counter
            if retries > 0:
                data["retries"] = retries - 1
                rdb.lpush(QUEUE_KEY, json.dumps(data))
                print(f"[Worker-{worker_id}] ↩ Re-queued '{task_type}', retries left: {retries - 1}")
            else:
                print(f"[Worker-{worker_id}] ✘ '{task_type}' exhausted all retries, dropping task")


if __name__ == "__main__":
    # Local dev: uvicorn will trigger lifespan automatically
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")
