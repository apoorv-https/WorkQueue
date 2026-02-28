from dataclasses import dataclass, field
from typing import Any


@dataclass
class Task:
    type: str
    payload: dict[str, Any] = field(default_factory=dict)
    retries: int = 0
