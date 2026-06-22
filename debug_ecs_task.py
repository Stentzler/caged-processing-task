from __future__ import annotations

import importlib
import os
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
ENV_FILE = PROJECT_ROOT / ".env"
LOCAL_CAGED_FILES_DIR = PROJECT_ROOT / "sample" / "caged_files"


def load_env_file(env_file: Path = ENV_FILE) -> None:
    """Load local debug configuration from a dotenv-style file."""
    if not env_file.exists():
        raise FileNotFoundError(f"{env_file} does not exist")

    for line in env_file.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        key, separator, value = stripped.partition("=")
        if not separator or not key.strip():
            continue

        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


class LocalSampleS3Client:
    """Local debug S3 stand-in that copies files from sample/caged_files."""

    def download_file(
        self,
        *,
        Bucket: str,
        Key: str,
        Filename: str,
        Config: object | None = None,
    ) -> None:
        source = LOCAL_CAGED_FILES_DIR / Path(Filename).name
        if not source.exists():
            raise FileNotFoundError(f"Local sample file not found: {source}")
        shutil.copyfile(source, Filename)


def use_local_caged_sample_files() -> bool:
    return os.getenv("LOCAL_CAGED_SAMPLE_FILES", "").lower() in {"1", "true", "yes"}


def load_cli_module() -> object:
    """Import the task entrypoint from src/app/cli.py."""
    sys.path.insert(0, str(SRC_DIR))
    return importlib.import_module("app.cli")


def main() -> None:
    load_env_file()

    cli_module = load_cli_module()
    if use_local_caged_sample_files():
        cli_module.get_s3_client = lambda settings: LocalSampleS3Client()

    raise SystemExit(cli_module.main())


if __name__ == "__main__":
    main()
