from __future__ import annotations

import json
from typing import Any, Protocol
from urllib.parse import urlparse

from serverless_toolkit.aws.dynamodb import DynamoDBSettings, get_dynamodb_table
from serverless_toolkit.aws.s3 import (
    S3Settings,
    get_s3_client,
)
from serverless_toolkit.observability.logger import get_logger

from app.exceptions import InvalidProcessingJobError
from app.processor import NoOpProcessor
from app.service import ProcessingService
from app.settings import Settings


class S3ObjectReader(Protocol):
    """S3 read operation required to load the processing job document."""

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]: ...


def main() -> int:
    settings = Settings()
    logger = get_logger(service="caged-processing-task", level=settings.LOG_LEVEL)
    dynamodb_settings = DynamoDBSettings(region_name=settings.AWS_REGION)
    s3_client = get_s3_client(S3Settings(region_name=settings.AWS_REGION))
    registry_table = get_dynamodb_table(
        settings.REGISTRY_TABLE_NAME,
        dynamodb_settings,
    )
    audit_table = get_dynamodb_table(
        settings.PROCESS_AUDIT_TABLE_NAME,
        dynamodb_settings,
    )

    try:
        event = load_event(settings, s3_client)
        service = ProcessingService(
            settings=settings,
            registry_table=registry_table,
            audit_table=audit_table,
            processor=NoOpProcessor(),
            logger=logger,
        )
        result = service.execute(event)
    except Exception:
        logger.exception("CAGED processing task failed")
        return 1

    logger.info("CAGED processing task finished: %s", json.dumps(result, default=str))
    return 0


def load_event(settings: Settings, s3_client: S3ObjectReader) -> dict[str, Any]:
    if settings.PROCESSING_JOB_JSON.strip():
        loaded = json.loads(settings.PROCESSING_JOB_JSON)
        if not isinstance(loaded, dict):
            raise ValueError("PROCESSING_JOB_JSON must be a JSON object")
        return loaded

    try:
        return load_processing_job_from_s3_uri(
            s3_client,
            settings.PROCESSING_JOB_S3_URI,
        )
    except ValueError as exc:
        raise InvalidProcessingJobError(str(exc)) from exc


def load_processing_job_from_s3_uri(
    s3_client: S3ObjectReader,
    s3_uri: str,
) -> dict[str, Any]:
    bucket, key = parse_processing_job_s3_uri(s3_uri)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    loaded = json.loads(body)
    if not isinstance(loaded, dict):
        msg = "S3 processing job document must be a JSON object"
        raise ValueError(msg)
    return loaded


def parse_processing_job_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        msg = "PROCESSING_JOB_S3_URI must be a valid S3 URI"
        raise ValueError(msg)
    return parsed.netloc, parsed.path.lstrip("/")
