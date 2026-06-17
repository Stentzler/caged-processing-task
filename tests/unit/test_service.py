from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from app.exceptions import ProcessingFailedError
from app.models import ProcessingMonth, ProcessingResult
from app.service import ProcessingService
from app.settings import Settings
from tests.unit.test_models import VALID_JOB


@dataclass
class FakeRegistryTable:
    item: dict[str, Any] = field(
        default_factory=lambda: {
            "Item": {
                "registry_id": "ftp_tree",
                "tree": {
                    "2026": {
                        "202604": {
                            file["filename"]: {
                                "status": file["status"],
                                "processing_status": "pending",
                                "s3_url": file["s3_uri"],
                            }
                            for file in VALID_JOB["files"]
                        }
                    }
                },
            }
        }
    )
    get_calls: list[dict[str, object]] = field(default_factory=list)
    update_calls: list[dict[str, object]] = field(default_factory=list)

    def get_item(self, **kwargs: object) -> dict[str, Any]:
        self.get_calls.append(kwargs)
        return self.item

    def update_item(self, **kwargs: object) -> dict[str, Any]:
        self.update_calls.append(kwargs)
        return {}


@dataclass
class FakeAuditTable:
    put_calls: list[dict[str, object]] = field(default_factory=list)

    def put_item(self, **kwargs: object) -> dict[str, Any]:
        self.put_calls.append(kwargs)
        return {}


@dataclass
class FakeLogger:
    messages: list[str] = field(default_factory=list)

    def info(self, message: str, *args: object, **kwargs: object) -> None:
        self.messages.append(message)

    def exception(self, message: str, *args: object, **kwargs: object) -> None:
        self.messages.append(message)


@dataclass
class FakeProcessor:
    error: Exception | None = None
    months: list[ProcessingMonth] = field(default_factory=list)

    def process(self, month: ProcessingMonth) -> ProcessingResult:
        self.months.append(month)
        if self.error:
            raise self.error
        return ProcessingResult(
            status="ok",
            details={"records": 10},
        )


def build_service(
    processor: FakeProcessor | None = None,
) -> tuple[ProcessingService, FakeRegistryTable, FakeAuditTable, FakeProcessor]:
    settings = Settings(
        REGISTRY_TABLE_NAME="downloaded_files_registry",
        REGISTRY_ID="ftp_tree",
        PROCESS_AUDIT_TABLE_NAME="caged_processes",
        PROCESSING_JOB_JSON="{}",
    )
    registry_table = FakeRegistryTable()
    audit_table = FakeAuditTable()
    effective_processor = processor or FakeProcessor()
    service = ProcessingService(
        settings=settings,
        registry_table=registry_table,
        audit_table=audit_table,
        processor=effective_processor,
        logger=FakeLogger(),
        timestamp_factory=lambda: "2026-06-15T12:00:00+00:00",
        uuid_factory=SequentialUUIDFactory(),
    )
    return service, registry_table, audit_table, effective_processor


@dataclass
class SequentialUUIDFactory:
    value: int = 0

    def __call__(self) -> str:
        self.value += 1
        return f"uuid-{self.value}"


def test_execute_marks_processing_and_writes_audit_records() -> None:
    service, registry_table, audit_table, processor = build_service()

    response = service.execute(VALID_JOB)

    assert len(processor.months) == 1
    assert response == {
        "status": "ok",
        "source_status": "COMPLETED",
        "months": [
            {
                "reference_month": "202604",
                "reference_year": "2026",
                "processing_status": "processing",
                "missing_file_types": [],
                "files": [
                    {
                        "filename": "CAGEDEXC202604.7z",
                        "file_type": "CAGEDEXC",
                        "process_id": "uuid-1",
                        "processing_status": "processing",
                    },
                    {
                        "filename": "CAGEDFOR202604.7z",
                        "file_type": "CAGEDFOR",
                        "process_id": "uuid-2",
                        "processing_status": "processing",
                    },
                    {
                        "filename": "CAGEDMOV202604.7z",
                        "file_type": "CAGEDMOV",
                        "process_id": "uuid-3",
                        "processing_status": "processing",
                    },
                ],
                "processor_result": {
                    "status": "ok",
                    "details": {"records": 10},
                },
            }
        ],
    }
    assert len(registry_table.get_calls) == 1
    assert len(registry_table.update_calls) == 3
    assert all(
        call["ExpressionAttributeValues"][":processing_status"] == "processing"
        for call in registry_table.update_calls
    )
    assert [
        call["ExpressionAttributeValues"][":process_id"]
        for call in registry_table.update_calls
    ] == ["uuid-1", "uuid-2", "uuid-3"]
    assert len(audit_table.put_calls) == 3
    assert audit_table.put_calls[0]["Item"]["reference_month"] == "202604"
    assert audit_table.put_calls[0]["Item"]["process_id"] == "uuid-1"


def test_execute_marks_incomplete_month_as_error_and_continues() -> None:
    service, registry_table, audit_table, processor = build_service()
    event = {
        "status": "COMPLETED",
        "files": [
            VALID_JOB["files"][0],
            VALID_JOB["files"][1],
            {
                "status": "downloaded",
                "filename": "CAGEDEXC202605.7z",
                "reference_month": "202605",
                "reference_year": "2026",
                "s3_bucket": "raw-bucket",
                "s3_key": "raw/caged/202605/CAGEDEXC202605.7z",
                "s3_uri": "s3://raw-bucket/raw/caged/202605/CAGEDEXC202605.7z",
                "size_bytes": 4,
            },
            {
                "status": "downloaded",
                "filename": "CAGEDFOR202605.7z",
                "reference_month": "202605",
                "reference_year": "2026",
                "s3_bucket": "raw-bucket",
                "s3_key": "raw/caged/202605/CAGEDFOR202605.7z",
                "s3_uri": "s3://raw-bucket/raw/caged/202605/CAGEDFOR202605.7z",
                "size_bytes": 5,
            },
            {
                "status": "downloaded",
                "filename": "CAGEDMOV202605.7z",
                "reference_month": "202605",
                "reference_year": "2026",
                "s3_bucket": "raw-bucket",
                "s3_key": "raw/caged/202605/CAGEDMOV202605.7z",
                "s3_uri": "s3://raw-bucket/raw/caged/202605/CAGEDMOV202605.7z",
                "size_bytes": 6,
            },
        ],
    }
    registry_table.item["Item"]["tree"]["2026"]["202605"] = {
        "CAGEDEXC202605.7z": {"status": "downloaded", "processing_status": "pending"},
        "CAGEDFOR202605.7z": {"status": "downloaded", "processing_status": "pending"},
        "CAGEDMOV202605.7z": {"status": "downloaded", "processing_status": "pending"},
    }

    response = service.execute(event)

    assert [month["processing_status"] for month in response["months"]] == [
        "error",
        "processing",
    ]
    assert response["months"][0]["missing_file_types"] == ["CAGEDMOV"]
    assert response["months"][1]["missing_file_types"] == []
    assert len(processor.months) == 1
    assert [
        call["ExpressionAttributeValues"][":processing_status"]
        for call in registry_table.update_calls
    ] == ["error", "error", "processing", "processing", "processing"]
    assert audit_table.put_calls[0]["Item"]["status"] == "error"
    assert audit_table.put_calls[0]["Item"]["missing_file_types"] == ["CAGEDMOV"]


def test_execute_raises_when_processor_fails() -> None:
    service, _, _, _ = build_service(
        processor=FakeProcessor(error=RuntimeError("parse error"))
    )

    with pytest.raises(ProcessingFailedError):
        service.execute(VALID_JOB)


def test_execute_raises_when_registry_file_entry_is_missing() -> None:
    service, registry_table, _, _ = build_service()
    del registry_table.item["Item"]["tree"]["2026"]["202604"]["CAGEDMOV202604.7z"]

    with pytest.raises(ProcessingFailedError, match="Missing registry file entry"):
        service.execute(VALID_JOB)
