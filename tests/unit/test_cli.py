from __future__ import annotations

import io
import json
from typing import Any

import pytest

from app.cli import load_event, load_processing_job_from_s3_uri
from app.exceptions import InvalidProcessingJobError
from app.settings import Settings


class FakeS3Client:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.requests: list[dict[str, str]] = []

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.requests.append({"bucket": Bucket, "key": Key})
        return {"Body": io.BytesIO(self.body)}


def test_load_event_uses_processing_job_json() -> None:
    event = {"status": "COMPLETED", "files": []}
    settings = Settings(PROCESSING_JOB_JSON=json.dumps(event))

    assert load_event(settings, FakeS3Client(b"{}")) == event


def test_load_processing_job_from_s3_uri_returns_json_object() -> None:
    s3_client = FakeS3Client(b'{"status": "COMPLETED", "files": []}')

    loaded = load_processing_job_from_s3_uri(
        s3_client,
        "s3://job-bucket/jobs/caged/2026-04.json",
    )

    assert loaded == {"status": "COMPLETED", "files": []}
    assert s3_client.requests == [
        {"bucket": "job-bucket", "key": "jobs/caged/2026-04.json"}
    ]


def test_load_event_rejects_non_object_s3_job_document() -> None:
    settings = Settings(PROCESSING_JOB_S3_URI="s3://job-bucket/jobs/job.json")

    with pytest.raises(InvalidProcessingJobError, match="must be a JSON object"):
        load_event(settings, FakeS3Client(b"[]"))
