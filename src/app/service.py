from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

from app.exceptions import ProcessingFailedError
from app.models import DownloadedFile, ProcessingJob, ProcessingMonth, ProcessingResult
from app.processor import ProcessingEngineProtocol
from app.settings import Settings

FINISHED_PROCESSING_STATUS = "finished"
TERMINAL_PROCESSING_STATUSES = frozenset({FINISHED_PROCESSING_STATUS})
CAGED_GEO_JOB_METRICS_DATASET_ID = "CAGED_GEO_JOB_METRICS"
DATASET_METADATA_SORT_KEY = "METADATA"


class RegistryTableProtocol(Protocol):
    def get_item(self, **kwargs: object) -> dict[str, Any]: ...

    def update_item(self, **kwargs: object) -> dict[str, Any]: ...


class AuditTableProtocol(Protocol):
    def put_item(self, **kwargs: object) -> dict[str, Any]: ...


class DatasetCatalogTableProtocol(Protocol):
    def get_item(self, **kwargs: object) -> dict[str, Any]: ...

    def update_item(self, **kwargs: object) -> dict[str, Any]: ...


class LoggerProtocol(Protocol):
    def info(self, message: str, *args: object, **kwargs: object) -> None: ...

    def exception(self, message: str, *args: object, **kwargs: object) -> None: ...


@dataclass(frozen=True)
class ProcessingFileResult:
    source_file: DownloadedFile = field(repr=False, compare=False)
    filename: str
    file_type: str
    process_id: str
    processing_status: str


@dataclass(frozen=True)
class ProcessingMonthResult:
    reference_month: str
    reference_year: str
    processing_status: str
    missing_file_types: list[str]
    files: list[ProcessingFileResult]
    processor_result: ProcessingResult


class ProcessingService:
    """Coordinate one batch of downloaded CAGED files."""

    def __init__(
        self,
        settings: Settings,
        registry_table: RegistryTableProtocol,
        audit_table: AuditTableProtocol,
        dataset_catalog_table: DatasetCatalogTableProtocol,
        processor: ProcessingEngineProtocol,
        logger: LoggerProtocol,
        timestamp_factory: Callable[[], str] | None = None,
        uuid_factory: Callable[[], str] | None = None,
    ) -> None:
        self.settings = settings
        self.registry_table = registry_table
        self.audit_table = audit_table
        self.dataset_catalog_table = dataset_catalog_table
        self.processor = processor
        self.logger = logger
        self.timestamp_factory = timestamp_factory or current_utc_timestamp
        self.uuid_factory = uuid_factory or current_uuid

    def execute(self, event: object) -> dict[str, Any]:
        job = ProcessingJob.from_mapping(event)

        try:
            registry_tree = self._load_registry_tree()

            grouped_months: list[ProcessingMonthResult] = [
                self._group_month(month) for month in job.group_by_reference_month()
            ]

            month_results = [
                self._process_month(month, registry_tree) for month in grouped_months
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
            "months": [serialize_month_result(month) for month in month_results],
        }

    def _process_month(
        self,
        month_files: ProcessingMonthResult,
        registry_tree: dict[str, Any],
    ) -> ProcessingMonthResult:
        if month_files.processing_status == "error":
            for file_result in month_files.files:
                self._write_audit_item(
                    file=file_result.source_file,
                    process_id=file_result.process_id,
                    processing_status=file_result.processing_status,
                    missing_file_types=month_files.missing_file_types,
                )
                self._update_registry_entry(
                    file=file_result.source_file,
                    process_id=file_result.process_id,
                    processing_status=file_result.processing_status,
                )
            return month_files

        registry_entries = [
            self._get_registry_entry(registry_tree, file_result.source_file)
            for file_result in month_files.files
        ]

        self._ensure_month_was_not_processed(month_files, registry_entries)

        processor_result = self.processor.process(month_files)
        self._update_dataset_catalog(month_files.reference_month)
        finished_files = [
            replace(file_result, processing_status=FINISHED_PROCESSING_STATUS)
            for file_result in month_files.files
        ]
        for file_result in finished_files:
            self._write_audit_item(
                file=file_result.source_file,
                process_id=file_result.process_id,
                processing_status=file_result.processing_status,
                missing_file_types=month_files.missing_file_types,
            )
            self._update_registry_entry(
                file=file_result.source_file,
                process_id=file_result.process_id,
                processing_status=file_result.processing_status,
            )
        return replace(
            month_files,
            processing_status=FINISHED_PROCESSING_STATUS,
            files=finished_files,
            processor_result=processor_result,
        )

    def _group_month(
        self,
        month: ProcessingMonth,
    ) -> ProcessingMonthResult:
        missing_file_types = month.missing_file_types
        month_status = "error" if missing_file_types else "processing"
        file_results: list[ProcessingFileResult] = []

        for file in month.files:
            process_id = self.uuid_factory()
            file_results.append(
                ProcessingFileResult(
                    source_file=file,
                    filename=file.filename,
                    file_type=file.file_type,
                    process_id=process_id,
                    processing_status=month_status,
                )
            )

        result = ProcessingResult(
            status="missing_files" if missing_file_types else "",
            details={},
        )

        return ProcessingMonthResult(
            reference_month=month.reference_month,
            reference_year=month.reference_year,
            processing_status=month_status,
            missing_file_types=missing_file_types,
            files=file_results,
            processor_result=result,
        )

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

    def _ensure_month_was_not_processed(
        self,
        month: ProcessingMonthResult,
        registry_entries: list[dict[str, Any]],
    ) -> None:
        processed_files = [
            file_result.filename
            for file_result, entry in zip(month.files, registry_entries, strict=True)
            if str(entry.get("processing_status", "")).lower()
            in TERMINAL_PROCESSING_STATUSES
        ]
        if processed_files:
            files = ", ".join(processed_files)
            raise ProcessingFailedError(
                "CAGED month already has processed files: "
                f"reference_month={month.reference_month} files={files}"
            )

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
                "#tree.#year.#month.#filename.#last_process_id = :last_process_id, "
                "#tree.#year.#month.#filename.#updated_at = :updated_at"
            ),
            ConditionExpression="attribute_exists(#tree.#year.#month.#filename)",
            ExpressionAttributeNames={
                "#tree": "tree",
                "#year": file.reference_year,
                "#month": file.reference_month,
                "#filename": file.filename,
                "#processing_status": "processing_status",
                "#last_process_id": "last_process_id",
                "#updated_at": "updated_at",
            },
            ExpressionAttributeValues={
                ":processing_status": processing_status,
                ":last_process_id": process_id,
                ":updated_at": timestamp,
            },
        )

    def _write_audit_item(
        self,
        *,
        file: DownloadedFile,
        process_id: str,
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
            "s3_bucket": file.s3_bucket,
            "s3_key": file.s3_key,
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

    def _update_dataset_catalog(self, reference_month: str) -> None:
        key = {
            "PK": f"DATASET#{CAGED_GEO_JOB_METRICS_DATASET_ID}",
            "SK": DATASET_METADATA_SORT_KEY,
        }
        response = self.dataset_catalog_table.get_item(
            Key=key,
            ConsistentRead=True,
        )
        item = response.get("Item")
        if not isinstance(item, dict):
            item = {}

        available_months = item.get("available_months")
        if isinstance(available_months, list):
            updated_months = list(available_months)
        else:
            updated_months = []

        if reference_month not in {str(month) for month in updated_months}:
            updated_months.append(reference_month)

        latest_available_month = str(item.get("latest_available_month") or "")
        if reference_month > latest_available_month:
            latest_available_month = reference_month

        self.dataset_catalog_table.update_item(
            Key=key,
            UpdateExpression=(
                "SET "
                "#available_months = :available_months, "
                "#latest_available_month = :latest_available_month, "
                "#updated_at = :updated_at"
            ),
            ExpressionAttributeNames={
                "#available_months": "available_months",
                "#latest_available_month": "latest_available_month",
                "#updated_at": "updated_at",
            },
            ExpressionAttributeValues={
                ":available_months": updated_months,
                ":latest_available_month": latest_available_month,
                ":updated_at": self.timestamp_factory(),
            },
        )


def current_utc_timestamp() -> str:
    return datetime.now(UTC).isoformat()


def current_uuid() -> str:
    return str(uuid4())


def serialize_month_result(month: ProcessingMonthResult) -> dict[str, Any]:
    return {
        "reference_month": month.reference_month,
        "reference_year": month.reference_year,
        "processing_status": month.processing_status,
        "missing_file_types": month.missing_file_types,
        "files": [
            {
                "filename": file.filename,
                "file_type": file.file_type,
                "process_id": file.process_id,
                "processing_status": file.processing_status,
            }
            for file in month.files
        ],
        "processor_result": {
            "status": month.processor_result.status,
            "details": month.processor_result.details,
        },
    }
