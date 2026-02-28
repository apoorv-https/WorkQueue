import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs.txt"),
        logging.StreamHandler(),
    ],
)
_log = logging.getLogger(__name__)


def log_success(task_type: str, payload: dict) -> None:
    _log.info(f"SUCCESS | type={task_type} | payload={json.dumps(payload)}")


def log_failure(task_type: str, payload: dict, error: Exception) -> None:
    _log.error(f"FAILURE | type={task_type} | payload={json.dumps(payload)} | error={error}")
