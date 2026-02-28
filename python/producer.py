"""
Producer Service — FastAPI
POST /enqueue  →  validates task and pushes to Redis queue
"""

import json
import os

import redis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, field_validator

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
QUEUE_KEY = "task_queue"

rdb = redis.from_url(REDIS_URL, decode_responses=True)
app = FastAPI(title="WorkQueue Producer")


# ---------- Request model ----------
class Task(BaseModel):
    type: str
    payload: dict = {}
    retries: int = 0

    @field_validator("type")
    @classmethod
    def type_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field 'type' must not be empty")
        return v


# ---------- Routes ----------
@app.post("/enqueue")
def enqueue(task: Task):
    # Extra validation for known task types
    if task.type == "send_email":
        if not task.payload.get("to") or not task.payload.get("subject"):
            raise HTTPException(
                status_code=400,
                detail="send_email requires 'to' and 'subject' in payload",
            )

    queue_len = rdb.rpush(QUEUE_KEY, task.model_dump_json())

    return {
        "message": f"Task '{task.type}' added to queue",
        "queue_length": queue_len,
    }


@app.get("/health")
def health():
    return {"status": "ok"}
