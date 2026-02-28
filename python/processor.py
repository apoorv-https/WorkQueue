"""
Task Registry — processor.py
Add a new task handler by decorating a function with @register("task_type").
No other file needs to change.
"""

import time
from typing import Callable

# Registry: maps task type string → handler function
_registry: dict[str, Callable[[dict], None]] = {}


def register(task_type: str):
    """Decorator to register a function as a task handler."""
    def decorator(func: Callable[[dict], None]):
        _registry[task_type] = func
        return func
    return decorator


def process_task(task_type: str, payload: dict) -> None:
    """Look up handler in registry and call it. Raises ValueError if unknown."""
    if not payload:
        raise ValueError("Payload is empty")

    handler = _registry.get(task_type)
    if handler is None:
        raise ValueError(f"Unsupported task type: '{task_type}'")
    handler(payload)


# ── Registered task handlers ───────────────────────────────────────────────

@register("send_email")
def send_email(payload: dict) -> None:
    time.sleep(2)  # simulate network call
    print(f"  ✉ Email → {payload.get('to')} | Subject: {payload.get('subject')}")


@register("resize_image")
def resize_image(payload: dict) -> None:
    print(f"  🖼 Resize image → x={payload.get('new_x')}, y={payload.get('new_y')}")


@register("generate_pdf")
def generate_pdf(payload: dict) -> None:
    print("  📄 Generating PDF...")


@register("process_data")
def process_data(payload: dict) -> None:
    item_id = payload.get("item_id")
    value   = payload.get("value")
    print(f"  ⚙ Processing data → item_id={item_id}, value={value}")
