from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

from app.exceptions import ProcessingFailedError
from app.models import DownloadedFile, ProcessingJob, ProcessingMonth
from app.processor import ProcessingEngineProtocol
from app.settings import Settings


class RegistryTableProtocol(Protocol):
    def get_item(self, **kwargs: object) -> dict[str, Any]: ...

    def update_item(self, **kwargs: object) -> dict[str, Any]: ...


class AuditTableProtocol(Protocol):
    def put_item(self, **kwargs: object) -> dict[str, Any]: ...


class LoggerProtocol(Protocol):
    def info(self, message: str, *args: object, **kwargs: object) -> None: ...

    def exception(self, message: str, *args: object, **kwargs: object) -> None: ...


class ProcessingService:
    """Coordinate one batch of downloaded CAGED files."""

    def __init__(
        self,
        settings: Settings,
        registry_table: RegistryTableProtocol,
        audit_table: AuditTableProtocol,
        processor: ProcessingEngineProtocol,
        logger: LoggerProtocol,
        timestamp_factory: Callable[[], str] | None = None,
        uuid_factory: Callable[[], str] | None = None,
    ) -> None:
        self.settings = settings
        self.registry_table = registry_table
        self.audit_table = audit_table
        self.processor = processor
        self.logger = logger
        self.timestamp_factory = timestamp_factory or current_utc_timestamp
        self.uuid_factory = uuid_factory or current_uuid

    def execute(self, event: object) -> dict[str, Any]:
        job = ProcessingJob.from_mapping(event)
        try:
            registry_tree = self._load_registry_tree()
            month_results = [
                self._process_month(month, registry_tree)
                for month in job.group_by_reference_month()
            ]
        except ProcessingFailedError:
            self.logger.exception("Failed CAGED file pre-processing")
            raise
        except Exception as error:
            self.logger.exception("Failed CAGED file pre-processing")
            raise ProcessingFailedError("CAGED processing failed") from error

        return {
            "status": "ok",
            "source_status": job.status,
            "months": month_results,
        }

    def _process_month(
        self,
        month: ProcessingMonth,
        registry_tree: dict[str, Any],
    ) -> dict[str, Any]:
        missing_file_types = month.missing_file_types
        month_status = "error" if missing_file_types else "processing"
        file_results = []

        for file in month.files:
            process_id = self.uuid_factory()
            registry_entry = self._get_registry_entry(registry_tree, file)
            self._update_registry_entry(file, process_id, month_status)
            self._write_audit_item(
                file=file,
                process_id=process_id,
                registry_entry=registry_entry,
                processing_status=month_status,
                missing_file_types=missing_file_types,
            )
            file_results.append(
                {
                    "filename": file.filename,
                    "file_type": file.file_type,
                    "process_id": process_id,
                    "processing_status": month_status,
                }
            )

        result = {"status": "ok", "details": {}}
        if month.is_complete:
            processor_result = self.processor.process(month)
            result = {
                "status": processor_result.status,
                "details": processor_result.details,
            }

        return {
            "reference_month": month.reference_month,
            "reference_year": month.reference_year,
            "processing_status": month_status,
            "missing_file_types": missing_file_types,
            "files": file_results,
            "processor_result": result,
        }

    def _load_registry_tree(self) -> dict[str, Any]:
        response = self.registry_table.get_item(
            Key={"registry_id": self.settings.REGISTRY_ID},
            ConsistentRead=True,
        )
        item = response.get("Item")
        if not item:
            raise ProcessingFailedError(
                f"Registry item {self.settings.REGISTRY_ID!r} was not found"
            )

        tree = item.get("tree")
        if not isinstance(tree, dict):
            raise ProcessingFailedError("Registry tree must be a mapping")
        return tree

    def _get_registry_entry(
        self,
        tree: dict[str, Any],
        file: DownloadedFile,
    ) -> dict[str, Any]:
        year = tree.get(file.reference_year)
        if not isinstance(year, dict):
            raise ProcessingFailedError(
                f"Missing registry year path for {file.reference_year}"
            )
        month = year.get(file.reference_month)
        if not isinstance(month, dict):
            raise ProcessingFailedError(
                f"Missing registry month path for {file.reference_month}"
            )
        entry = month.get(file.filename)
        if not isinstance(entry, dict):
            raise ProcessingFailedError(
                f"Missing registry file entry for {file.filename}"
            )
        return entry

    def _update_registry_entry(
        self,
        file: DownloadedFile,
        process_id: str,
        processing_status: str,
    ) -> None:
        timestamp = self.timestamp_factory()
        self.registry_table.update_item(
            Key={"registry_id": self.settings.REGISTRY_ID},
            UpdateExpression=(
                "SET "
                "#tree.#year.#month.#filename.#processing_status = :processing_status, "
                "#tree.#year.#month.#filename.#process_id = :process_id, "
                "#tree.#year.#month.#filename.#updated_at = :updated_at"
            ),
            ConditionExpression="attribute_exists(#tree.#year.#month.#filename)",
            ExpressionAttributeNames={
                "#tree": "tree",
                "#year": file.reference_year,
                "#month": file.reference_month,
                "#filename": file.filename,
                "#processing_status": "processing_status",
                "#process_id": "process_id",
                "#updated_at": "updated_at",
            },
            ExpressionAttributeValues={
                ":processing_status": processing_status,
                ":process_id": process_id,
                ":updated_at": timestamp,
            },
        )

    def _write_audit_item(
        self,
        *,
        file: DownloadedFile,
        process_id: str,
        registry_entry: dict[str, Any],
        processing_status: str,
        missing_file_types: list[str],
    ) -> None:
        timestamp = self.timestamp_factory()
        item = {
            "reference_month": file.reference_month,
            "process_id": process_id,
            "reference_year": file.reference_year,
            "filename": file.filename,
            "file_type": file.file_type,
            "status": processing_status,
            "source_status": file.status,
            "registry_status": registry_entry.get("status"),
            "s3_bucket": file.s3_bucket,
            "s3_key": file.s3_key,
            "s3_uri": file.s3_uri,
            "size_bytes": file.size_bytes,
            "created_at": timestamp,
            "updated_at": timestamp,
        }
        if missing_file_types:
            item["error_reason"] = "Missing required files for month: " + ", ".join(
                missing_file_types
            )
            item["missing_file_types"] = missing_file_types
        self.audit_table.put_item(Item=item)


def current_utc_timestamp() -> str:
    return datetime.now(UTC).isoformat()


def current_uuid() -> str:
    return str(uuid4())
