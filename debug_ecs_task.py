from __future__ import annotations

import importlib
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
DEFAULT_EVENT_PATH = PROJECT_ROOT / "sample" / "received_payload.json"

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "dummy")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "dummy")
os.environ.setdefault("DYNAMODB_ENDPOINT_URL", "http://127.0.0.1:8000")
os.environ.setdefault("REGISTRY_TABLE_NAME", "downloaded_files_registry")
os.environ.setdefault("REGISTRY_ID", "ftp_tree")
os.environ.setdefault("PROCESS_AUDIT_TABLE_NAME", "caged_processes")
os.environ.setdefault("LOG_LEVEL", "DEBUG")


def load_processing_job_json(event_path: Path = DEFAULT_EVENT_PATH) -> str:
    """Return the local processing payload used to simulate the ECS task input."""
    if not event_path.exists():
        return ""

    event_text = event_path.read_text()
    if not event_text.strip():
        return ""

    loaded = json.loads(event_text)
    return json.dumps(loaded)


def load_main() -> object:
    """Import the task entrypoint from src/app/cli.py."""
    sys.path.insert(0, str(SRC_DIR))
    cli_module = importlib.import_module("app.cli")
    return cli_module.main


def main() -> None:
    os.environ.setdefault("PROCESSING_JOB_JSON", load_processing_job_json())

    task_main = load_main()
    raise SystemExit(task_main())


if __name__ == "__main__":
    main()
