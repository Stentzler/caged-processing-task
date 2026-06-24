from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from itertools import batched
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter, sleep
from typing import TYPE_CHECKING, Any, Protocol, TextIO

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from app.models import ProcessingResult

if TYPE_CHECKING:
    from app.service import ProcessingMonthResult

REQUIRED_FILE_TYPES = frozenset({"CAGEDMOV", "CAGEDFOR", "CAGEDEXC"})
ADMISSION_MOVEMENT = "1"
DISMISSAL_MOVEMENT = "-1"
ALL_FAMILY_CODE = "ALL"
ALL_FAMILY_TITLE = "All professions"
UNKNOWN_LABEL = "UNKNOWN"
MONEY_QUANTIZER = Decimal("0.01")
METRICS_PER_TRANSACTION = 50
BATCH_GET_MAX_ATTEMPTS = 4
BATCH_GET_BASE_DELAY_SECONDS = 0.05
S3_DOWNLOAD_CONFIG = TransferConfig(
    max_concurrency=2,
    num_download_attempts=10,
)


class ProcessingEngineProtocol(Protocol):
    def process(self, month: ProcessingMonthResult) -> ProcessingResult: ...


class S3ObjectDownloader(Protocol):
    def download_file(
        self,
        *,
        Bucket: str,
        Key: str,
        Filename: str,
        Config: TransferConfig | None = None,
    ) -> None: ...


class DynamoDBLookupTableProtocol(Protocol):
    def get_item(self, **kwargs: object) -> dict[str, Any]: ...


class DynamoDBBatchWriterProtocol(Protocol):
    def __enter__(self) -> DynamoDBBatchWriterProtocol: ...

    def __exit__(self, *args: object) -> None: ...

    def put_item(self, **kwargs: object) -> dict[str, Any]: ...


class DynamoDBClientProtocol(Protocol):
    def batch_get_item(self, **kwargs: object) -> dict[str, Any]: ...

    def transact_write_items(self, **kwargs: object) -> dict[str, Any]: ...


class DynamoDBTableMetaProtocol(Protocol):
    client: DynamoDBClientProtocol


class DynamoDBMetricsTableProtocol(Protocol):
    name: str
    meta: DynamoDBTableMetaProtocol

    def batch_writer(self) -> DynamoDBBatchWriterProtocol: ...

    def get_item(self, **kwargs: object) -> dict[str, Any]: ...

    def put_item(self, **kwargs: object) -> dict[str, Any]: ...


class DynamoDBRevisionTableProtocol(Protocol):
    name: str
    meta: DynamoDBTableMetaProtocol

    def put_item(self, **kwargs: object) -> dict[str, Any]: ...

    def query(self, **kwargs: object) -> dict[str, Any]: ...

    def update_item(self, **kwargs: object) -> dict[str, Any]: ...


class DynamoDBMetricBatchTableProtocol(Protocol):
    name: str
    meta: DynamoDBTableMetaProtocol

    def put_item(self, **kwargs: object) -> dict[str, Any]: ...

    def query(self, **kwargs: object) -> dict[str, Any]: ...


class LoggerProtocol(Protocol):
    def info(self, message: str, *args: object, **kwargs: object) -> None: ...

    def warning(self, message: str, *args: object, **kwargs: object) -> None: ...

    def debug(self, message: str, *args: object, **kwargs: object) -> None: ...


@dataclass(frozen=True)
class LocalCagedFile:
    filename: str
    file_type: str
    path: Path


@dataclass(frozen=True)
class ProfessionInfo:
    family_code: str
    family_title: str


@dataclass(frozen=True)
class LocationInfo:
    location_type: str
    location_code: str
    location_name: str
    state_code: str
    state_name: str
    region_name: str


@dataclass
class MetricAggregate:
    admissions: int = 0
    dismissals: int = 0
    salary_sum: Decimal = Decimal("0")
    salary_count: int = 0

    def apply(self, movement: str, salary: Decimal, multiplier: int) -> None:
        if movement == ADMISSION_MOVEMENT:
            self.admissions += multiplier
            self.salary_sum += salary * multiplier
            self.salary_count += multiplier
            return

        if movement == DISMISSAL_MOVEMENT:
            self.dismissals += multiplier

    def merge(self, other: MetricAggregate) -> None:
        self.admissions += other.admissions
        self.dismissals += other.dismissals
        self.salary_sum += other.salary_sum
        self.salary_count += other.salary_count

    @property
    def net_balance(self) -> int:
        return self.admissions - self.dismissals

    @property
    def total_turnover(self) -> int:
        return self.admissions + self.dismissals

    @property
    def avg_salary(self) -> Decimal:
        if self.salary_count == 0:
            return Decimal("0.00")
        return (self.salary_sum / Decimal(self.salary_count)).quantize(MONEY_QUANTIZER)


@dataclass(frozen=True)
class MetricKey:
    location: LocationInfo
    reference_month: str
    family_code: str
    family_title: str


@dataclass
class ProcessingStats:
    parsed_rows_by_file_type: dict[str, int]
    missing_cbo_codes: set[str]
    missing_geo_codes: set[str]
    new_metric_batches: int = 0
    skipped_metric_batches: int = 0
    applied_metric_batches: int = 0
    merged_metric_batches: int = 0
    new_metric_revisions: int = 0
    skipped_metric_revisions: int = 0
    applied_metric_revisions: int = 0
    merged_metric_revisions: int = 0

    @classmethod
    def empty(cls) -> ProcessingStats:
        return cls(
            parsed_rows_by_file_type={
                "CAGEDMOV": 0,
                "CAGEDFOR": 0,
                "CAGEDEXC": 0,
            },
            missing_cbo_codes=set(),
            missing_geo_codes=set(),
        )

    def as_details(self) -> dict[str, Any]:
        return {
            "parsed_rows_by_file_type": self.parsed_rows_by_file_type,
            "missing_cbo_lookup_count": len(self.missing_cbo_codes),
            "missing_geo_lookup_count": len(self.missing_geo_codes),
            "new_metric_batches": self.new_metric_batches,
            "skipped_metric_batches": self.skipped_metric_batches,
            "applied_metric_batches": self.applied_metric_batches,
            "merged_metric_batches": self.merged_metric_batches,
            "new_metric_revisions": self.new_metric_revisions,
            "skipped_metric_revisions": self.skipped_metric_revisions,
            "applied_metric_revisions": self.applied_metric_revisions,
            "merged_metric_revisions": self.merged_metric_revisions,
        }


class CagedProcessor:
    def __init__(
        self,
        s3_client: S3ObjectDownloader,
        geo_job_metrics_table: DynamoDBMetricsTableProtocol,
        metric_batches_table: DynamoDBMetricBatchTableProtocol,
        metric_revisions_table: DynamoDBRevisionTableProtocol,
        cbo_lookup_table: DynamoDBLookupTableProtocol,
        geo_lookup_table: DynamoDBLookupTableProtocol,
        logger: LoggerProtocol,
    ) -> None:
        self.s3_client = s3_client
        self.geo_job_metrics_table = geo_job_metrics_table
        self.metric_batches_table = metric_batches_table
        self.metric_revisions_table = metric_revisions_table
        self.cbo_lookup_table = cbo_lookup_table
        self.geo_lookup_table = geo_lookup_table
        self.logger = logger
        self._profession_cache: dict[str, ProfessionInfo] = {}
        self._location_cache: dict[tuple[str, str], LocationInfo] = {}
        self._missing_cbo_codes: set[str] = set()
        self._missing_geo_codes: set[str] = set()

    def process(self, month: ProcessingMonthResult) -> ProcessingResult:
        with TemporaryDirectory() as temporary_directory:
            local_files = self._download_files(month, Path(temporary_directory))
            details = self._process_files(local_files, month.reference_month)

        return ProcessingResult(
            status="ok",
            details={
                "processor": "caged",
                "reference_month": month.reference_month,
                "downloaded_files": len(local_files),
                **details,
            },
        )

    def _process_files(
        self,
        files: list[LocalCagedFile],
        source_month: str,
    ) -> dict[str, Any]:
        files_by_type = self._index_files_by_type(files)
        aggregates: dict[MetricKey, MetricAggregate] = {}
        stats = ProcessingStats.empty()

        for file_type in ("CAGEDMOV", "CAGEDFOR", "CAGEDEXC"):
            multiplier = -1 if file_type == "CAGEDEXC" else 1
            self.logger.info(
                "Starting CAGED file processing: file_type=%s filename=%s",
                file_type,
                files_by_type[file_type].filename,
            )

            for row in self._iter_file_rows(files_by_type[file_type]):
                stats.parsed_rows_by_file_type[file_type] += 1
                self._accumulate_row(aggregates, row, multiplier, stats)

            self.logger.info(
                "Finished CAGED file processing: file_type=%s filename=%s rows=%s",
                file_type,
                files_by_type[file_type].filename,
                stats.parsed_rows_by_file_type[file_type],
            )

        metric_batch_items = [
            self._build_metric_batch_item(source_month, key, aggregate)
            for key, aggregate in aggregates.items()
            if key.reference_month == source_month
        ]
        self.logger.info(
            "Writing CAGED metric batch items: count=%s",
            len(metric_batch_items),
        )

        self._write_metric_batch_items(metric_batch_items, stats)
        self.logger.info(
            "Finished writing CAGED metric batch items: created=%s skipped=%s",
            stats.new_metric_batches,
            stats.skipped_metric_batches,
        )

        self._apply_pending_metric_batch_items(source_month, stats)
        self.logger.info(
            "Finished applying CAGED metric batch items: applied=%s merged=%s",
            stats.applied_metric_batches,
            stats.merged_metric_batches,
        )

        revision_items = [
            self._build_revision_item(source_month, key, aggregate)
            for key, aggregate in aggregates.items()
            if key.reference_month != source_month
        ]
        self.logger.info(
            "Writing CAGED metric revision items: count=%s",
            len(revision_items),
        )
        self._write_revision_items(revision_items, stats)
        self.logger.info(
            "Finished writing CAGED metric revision items: created=%s skipped=%s",
            stats.new_metric_revisions,
            stats.skipped_metric_revisions,
        )

        self._apply_pending_revision_items(source_month, stats)
        self.logger.info(
            "Finished applying CAGED metric revision items: applied=%s merged=%s",
            stats.applied_metric_revisions,
            stats.merged_metric_revisions,
        )
        return stats.as_details()

    def _index_files_by_type(
        self,
        files: Iterable[LocalCagedFile],
    ) -> dict[str, LocalCagedFile]:
        files_by_type = {file.file_type: file for file in files}
        missing_file_types = REQUIRED_FILE_TYPES - set(files_by_type)
        if missing_file_types:
            missing = ", ".join(sorted(missing_file_types))
            raise ValueError(f"Missing required CAGED files: {missing}")
        return files_by_type

    def _iter_file_rows(self, file: LocalCagedFile) -> Iterator[dict[str, str]]:
        for csv_path in self._csv_paths(file.path):
            yield from self._read_csv_rows(csv_path)

    def _csv_paths(self, path: Path) -> list[Path]:
        if path.suffix.lower() in {".csv", ".txt"}:
            return [path]

        if path.suffix.lower() != ".7z":
            raise ValueError(f"Unsupported CAGED file extension: {path.name}")

        extracted_directory = path.with_suffix("")
        extracted_directory.mkdir(exist_ok=True)
        try:
            import py7zr
        except ImportError as exc:
            raise RuntimeError("py7zr is required to extract CAGED .7z files") from exc

        with py7zr.SevenZipFile(path, mode="r") as archive:
            archive.extractall(path=extracted_directory)

        caged_data_paths = sorted(
            path
            for path in extracted_directory.rglob("*")
            if path.suffix.lower() in {".csv", ".txt"}
        )
        if not caged_data_paths:
            raise ValueError(f"No CSV/TXT data file found inside archive: {path.name}")
        return caged_data_paths

    def _read_csv_rows(self, path: Path) -> Iterator[dict[str, str]]:
        with open_caged_csv(path) as csv_file:
            reader = csv.DictReader(csv_file, delimiter=";")
            for row in reader:
                yield {
                    key: value.strip() for key, value in row.items() if key is not None
                }

    def _accumulate_row(
        self,
        aggregates: dict[MetricKey, MetricAggregate],
        row: dict[str, str],
        multiplier: int,
        stats: ProcessingStats,
    ) -> None:
        """Apply one raw CAGED row to every dashboard aggregate it affects.

        A single employment movement belongs to both a city and a state, and it
        also contributes to both its specific CBO family and the location total.
        That means one row intentionally updates four independent metric
        buckets: city+family, state+family, city+ALL, and state+ALL. This is not
        double counting because each bucket is queried separately by the API.
        """
        movement = row["saldomovimentação"]
        if movement not in {ADMISSION_MOVEMENT, DISMISSAL_MOVEMENT}:
            return

        reference_month = row["competênciamov"]
        salary = parse_decimal(row.get("salário", "0"))
        profession = self._get_profession(row["cbo2002ocupação"], stats)
        city = self._get_city_location(row["município"], row["uf"], stats)
        state = self._get_state_location(row["uf"], city, stats)

        for location in (city, state):
            self._apply_metric(
                aggregates=aggregates,
                location=location,
                reference_month=reference_month,
                family_code=profession.family_code,
                family_title=profession.family_title,
                movement=movement,
                salary=salary,
                multiplier=multiplier,
            )
            self._apply_metric(
                aggregates=aggregates,
                location=location,
                reference_month=reference_month,
                family_code=ALL_FAMILY_CODE,
                family_title=ALL_FAMILY_TITLE,
                movement=movement,
                salary=salary,
                multiplier=multiplier,
            )

    def _apply_metric(
        self,
        *,
        aggregates: dict[MetricKey, MetricAggregate],
        location: LocationInfo,
        reference_month: str,
        family_code: str,
        family_title: str,
        movement: str,
        salary: Decimal,
        multiplier: int,
    ) -> None:
        key = MetricKey(
            location=location,
            reference_month=reference_month,
            family_code=family_code,
            family_title=family_title,
        )
        aggregate = aggregates.setdefault(key, MetricAggregate())
        aggregate.apply(movement, salary, multiplier)

    def _get_profession(
        self,
        cbo_code: str,
        stats: ProcessingStats,
    ) -> ProfessionInfo:
        normalized_cbo_code = cbo_code.zfill(6)
        family_code = normalized_cbo_code[:4]
        cached = self._profession_cache.get(family_code)
        if cached is not None:
            return cached

        item = self._get_cbo_lookup_item(family_code)
        if item is None:
            item = self._get_cbo_lookup_item(normalized_cbo_code)

        if isinstance(item, dict):
            family_code = str(item.get("family_code") or family_code)
            family_title = str(item.get("family_title") or UNKNOWN_LABEL)
            profession = ProfessionInfo(
                family_code=family_code,
                family_title=family_title,
            )
        else:
            profession = ProfessionInfo(
                family_code=family_code,
                family_title=UNKNOWN_LABEL,
            )
            self._warn_missing_cbo_family(family_code, normalized_cbo_code, stats)

        self._profession_cache[profession.family_code] = profession
        return profession

    def _get_cbo_lookup_item(self, code: str) -> dict[str, object] | None:
        response = self.cbo_lookup_table.get_item(Key={"code": code})
        item = response.get("Item")
        if isinstance(item, dict):
            return item
        return None

    def _get_city_location(
        self,
        city_code: str,
        state_code: str,
        stats: ProcessingStats,
    ) -> LocationInfo:
        cached = self._location_cache.get(("CITY", city_code))
        if cached is not None:
            return cached

        response = self.geo_lookup_table.get_item(
            Key={"code": city_code, "type": "CITY"}
        )
        item = response.get("Item")
        if isinstance(item, dict):
            location = LocationInfo(
                location_type="CITY",
                location_code=city_code,
                location_name=str(item.get("name") or UNKNOWN_LABEL),
                state_code=str(item.get("state_code") or state_code),
                state_name=str(item.get("state_name") or UNKNOWN_LABEL),
                region_name=str(item.get("region_name") or UNKNOWN_LABEL),
            )
        else:
            location = LocationInfo(
                location_type="CITY",
                location_code=city_code,
                location_name=UNKNOWN_LABEL,
                state_code=state_code,
                state_name=UNKNOWN_LABEL,
                region_name=UNKNOWN_LABEL,
            )
            self._warn_missing_geo(city_code, stats)

        self._location_cache[("CITY", city_code)] = location
        return location

    def _get_state_location(
        self,
        state_code: str,
        city: LocationInfo,
        stats: ProcessingStats,
    ) -> LocationInfo:
        cached = self._location_cache.get(("STATE", state_code))
        if cached is not None:
            return cached

        response = self.geo_lookup_table.get_item(
            Key={"code": state_code, "type": "STATE"}
        )
        item = response.get("Item")
        if isinstance(item, dict):
            location_name = str(
                item.get("name") or item.get("state_name") or UNKNOWN_LABEL
            )
            location = LocationInfo(
                location_type="STATE",
                location_code=state_code,
                location_name=location_name,
                state_code=state_code,
                state_name=str(item.get("state_name") or location_name),
                region_name=str(item.get("region_name") or UNKNOWN_LABEL),
            )
        elif city.state_name != UNKNOWN_LABEL:
            location = LocationInfo(
                location_type="STATE",
                location_code=state_code,
                location_name=city.state_name,
                state_code=state_code,
                state_name=city.state_name,
                region_name=city.region_name,
            )
        else:
            location = LocationInfo(
                location_type="STATE",
                location_code=state_code,
                location_name=UNKNOWN_LABEL,
                state_code=state_code,
                state_name=UNKNOWN_LABEL,
                region_name=UNKNOWN_LABEL,
            )
            self._warn_missing_geo(state_code, stats)

        self._location_cache[("STATE", state_code)] = location
        return location

    def _warn_missing_cbo_family(
        self,
        family_code: str,
        cbo_code: str,
        stats: ProcessingStats,
    ) -> None:
        stats.missing_cbo_codes.add(family_code)
        if family_code in self._missing_cbo_codes:
            return
        self._missing_cbo_codes.add(family_code)
        self.logger.warning(
            "Missing CBO family lookup: family_code=%s cbo_code=%s",
            family_code,
            cbo_code,
        )

    def _warn_missing_geo(self, geo_code: str, stats: ProcessingStats) -> None:
        stats.missing_geo_codes.add(geo_code)
        if geo_code in self._missing_geo_codes:
            return
        self._missing_geo_codes.add(geo_code)
        self.logger.warning("Missing geo lookup: code=%s", geo_code)

    def _build_metric_item(
        self,
        key: MetricKey,
        aggregate: MetricAggregate,
    ) -> dict[str, object]:
        pk = (
            f"LOC#{key.location.location_type}#{key.location.location_code}"
            f"#MONTH#{key.reference_month}"
        )
        sk = f"PROF#{key.family_code}"
        return {
            "PK": pk,
            "SK": sk,
            "location_type": key.location.location_type,
            "location_code": key.location.location_code,
            "location_name": key.location.location_name,
            "state_code": key.location.state_code,
            "state_name": key.location.state_name,
            "region_name": key.location.region_name,
            "reference_month": key.reference_month,
            "family_code": key.family_code,
            "family_title": key.family_title,
            "admissions": aggregate.admissions,
            "dismissals": aggregate.dismissals,
            "net_balance": aggregate.net_balance,
            "total_turnover": aggregate.total_turnover,
            "salary_sum": aggregate.salary_sum.quantize(MONEY_QUANTIZER),
            "salary_count": aggregate.salary_count,
            "avg_salary": aggregate.avg_salary,
            "GSI1_PK": f"MONTH#{key.reference_month}#PROF#{key.family_code}",
            "GSI1_SK": (
                f"NET#{aggregate.net_balance:+012d}"
                f"#LOC#{key.location.location_type}#{key.location.location_code}"
            ),
        }

    def _build_revision_item(
        self,
        source_month: str,
        key: MetricKey,
        aggregate: MetricAggregate,
    ) -> dict[str, object]:
        metric_item = self._build_metric_item(key, aggregate)
        created_at = utc_now_iso()
        return {
            "PK": f"REVISION_BATCH#{source_month}",
            "SK": f"METRIC#{metric_item['PK']}#{metric_item['SK']}",
            "source_month": source_month,
            "target_month": key.reference_month,
            "metric_pk": metric_item["PK"],
            "metric_sk": metric_item["SK"],
            "location_type": key.location.location_type,
            "location_code": key.location.location_code,
            "location_name": key.location.location_name,
            "state_code": key.location.state_code,
            "state_name": key.location.state_name,
            "region_name": key.location.region_name,
            "family_code": key.family_code,
            "family_title": key.family_title,
            "admissions_delta": aggregate.admissions,
            "dismissals_delta": aggregate.dismissals,
            "salary_sum_delta": aggregate.salary_sum.quantize(MONEY_QUANTIZER),
            "salary_count_delta": aggregate.salary_count,
            "status": "pending",
            "created_at": created_at,
            "updated_at": created_at,
        }

    def _build_metric_batch_item(
        self,
        source_month: str,
        key: MetricKey,
        aggregate: MetricAggregate,
    ) -> dict[str, object]:
        metric_item = self._build_metric_item(key, aggregate)
        created_at = utc_now_iso()
        return {
            "PK": f"BATCH#{source_month}",
            "SK": f"METRIC#{metric_item['PK']}#{metric_item['SK']}",
            "source_month": source_month,
            "target_month": key.reference_month,
            "metric_pk": metric_item["PK"],
            "metric_sk": metric_item["SK"],
            "location_type": key.location.location_type,
            "location_code": key.location.location_code,
            "location_name": key.location.location_name,
            "state_code": key.location.state_code,
            "state_name": key.location.state_name,
            "region_name": key.location.region_name,
            "family_code": key.family_code,
            "family_title": key.family_title,
            "admissions_delta": aggregate.admissions,
            "dismissals_delta": aggregate.dismissals,
            "salary_sum_delta": aggregate.salary_sum.quantize(MONEY_QUANTIZER),
            "salary_count_delta": aggregate.salary_count,
            "status": "pending",
            "created_at": created_at,
            "updated_at": created_at,
        }

    def _write_metric_batch_items(
        self,
        items: Iterable[dict[str, object]],
        stats: ProcessingStats,
    ) -> None:
        for item in items:
            try:
                self.metric_batches_table.put_item(
                    Item=item,
                    ConditionExpression=(
                        "attribute_not_exists(PK) AND attribute_not_exists(SK)"
                    ),
                )
                stats.new_metric_batches += 1
            except ClientError as error:
                if is_conditional_check_failure(error):
                    stats.skipped_metric_batches += 1
                    continue
                raise

    def _apply_pending_metric_batch_items(
        self,
        source_month: str,
        stats: ProcessingStats,
    ) -> None:
        batch_pk = f"BATCH#{source_month}"
        applied_count, merged_count = self._apply_pending_metric_delta_items(
            source_table=self.metric_batches_table,
            partition_key=batch_pk,
        )
        stats.applied_metric_batches += applied_count
        stats.merged_metric_batches += merged_count

    def _write_revision_items(
        self,
        items: Iterable[dict[str, object]],
        stats: ProcessingStats,
    ) -> None:
        for item in items:
            try:
                self.metric_revisions_table.put_item(
                    Item=item,
                    ConditionExpression=(
                        "attribute_not_exists(PK) AND attribute_not_exists(SK)"
                    ),
                )
                stats.new_metric_revisions += 1
            except ClientError as error:
                if is_conditional_check_failure(error):
                    stats.skipped_metric_revisions += 1
                    continue
                raise

    def _apply_pending_revision_items(
        self,
        source_month: str,
        stats: ProcessingStats,
    ) -> None:
        revision_batch_pk = f"REVISION_BATCH#{source_month}"
        applied_count, merged_count = self._apply_pending_metric_delta_items(
            source_table=self.metric_revisions_table,
            partition_key=revision_batch_pk,
        )
        stats.applied_metric_revisions += applied_count
        stats.merged_metric_revisions += merged_count

    def _apply_pending_metric_delta_items(
        self,
        *,
        source_table: DynamoDBMetricBatchTableProtocol | DynamoDBRevisionTableProtocol,
        partition_key: str,
    ) -> tuple[int, int]:
        pending_items = (
            item
            for item in self._query_metric_delta_items(source_table, partition_key)
            if item.get("status") != "applied"
        )
        applied_count = 0
        merged_count = 0

        for item_batch in batched(
            pending_items,
            METRICS_PER_TRANSACTION,
            strict=False,
        ):
            self.logger.debug(
                "Applying CAGED metric delta batch: partition_key=%s item_count=%s",
                partition_key,
                len(item_batch),
            )
            merged_count += self._apply_metric_delta_batch(
                source_table=source_table,
                delta_items=item_batch,
            )
            applied_count += len(item_batch)

        return applied_count, merged_count

    def _query_metric_delta_items(
        self,
        table: DynamoDBMetricBatchTableProtocol | DynamoDBRevisionTableProtocol,
        partition_key: str,
    ) -> Iterator[dict[str, Any]]:
        query_params: dict[str, object] = {
            "KeyConditionExpression": "PK = :pk",
            "ExpressionAttributeValues": {":pk": partition_key},
            "ConsistentRead": True,
        }

        while True:
            response = table.query(**query_params)
            yield from response.get("Items", [])

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break
            query_params["ExclusiveStartKey"] = last_evaluated_key

    def _apply_metric_delta_item(
        self,
        *,
        source_table: DynamoDBMetricBatchTableProtocol | DynamoDBRevisionTableProtocol,
        delta_item: dict[str, Any],
    ) -> bool:
        merged_count = self._apply_metric_delta_batch(
            source_table=source_table,
            delta_items=(delta_item,),
        )
        return merged_count == 1

    def _apply_metric_delta_batch(
        self,
        *,
        source_table: DynamoDBMetricBatchTableProtocol | DynamoDBRevisionTableProtocol,
        delta_items: tuple[dict[str, Any], ...],
    ) -> int:
        existing_items = self._batch_get_metric_items(delta_items)
        transaction_items: list[dict[str, Any]] = []
        merged_count = 0
        updated_at = utc_now_iso()

        for delta_item in delta_items:
            metric_key = (
                str(delta_item["metric_pk"]),
                str(delta_item["metric_sk"]),
            )
            existing_item = existing_items.get(metric_key)
            if existing_item is not None:
                merged_count += 1
            merged_item = self._merge_metric_delta_item(delta_item, existing_item)
            transaction_items.extend(
                [
                    {
                        "Put": {
                            "TableName": self.geo_job_metrics_table.name,
                            "Item": merged_item,
                        }
                    },
                    {
                        "Update": {
                            "TableName": source_table.name,
                            "Key": {
                                "PK": delta_item["PK"],
                                "SK": delta_item["SK"],
                            },
                            "UpdateExpression": (
                                "SET #status = :status, updated_at = :now"
                            ),
                            "ConditionExpression": "#status = :pending",
                            "ExpressionAttributeNames": {"#status": "status"},
                            "ExpressionAttributeValues": {
                                ":status": "applied",
                                ":pending": "pending",
                                ":now": updated_at,
                            },
                        }
                    },
                ]
            )

        try:
            self.geo_job_metrics_table.meta.client.transact_write_items(
                TransactItems=transaction_items
            )
        except ClientError as error:
            self.logger.debug(
                "Failed DynamoDB metric batch transaction: error=%s "
                "source_table=%s item_count=%s delta_items=%s "
                "transaction_items=%s",
                error.response.get("Error", {}),
                source_table.name,
                len(delta_items),
                delta_items,
                transaction_items,
            )
            raise
        return merged_count

    def _batch_get_metric_items(
        self,
        delta_items: tuple[dict[str, Any], ...],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        table_name = self.geo_job_metrics_table.name
        request_items: dict[str, Any] = {
            table_name: {
                "Keys": [
                    {
                        "PK": delta_item["metric_pk"],
                        "SK": delta_item["metric_sk"],
                    }
                    for delta_item in delta_items
                ],
                "ConsistentRead": True,
            }
        }
        existing_items: dict[tuple[str, str], dict[str, Any]] = {}

        for attempt in range(BATCH_GET_MAX_ATTEMPTS):
            response = self.geo_job_metrics_table.meta.client.batch_get_item(
                RequestItems=request_items
            )
            for item in response.get("Responses", {}).get(table_name, []):
                existing_items[(str(item["PK"]), str(item["SK"]))] = item

            unprocessed_keys = response.get("UnprocessedKeys", {})
            if not unprocessed_keys:
                return existing_items

            if attempt == BATCH_GET_MAX_ATTEMPTS - 1:
                break

            delay_seconds = BATCH_GET_BASE_DELAY_SECONDS * (2**attempt)
            self.logger.debug(
                "Retrying unprocessed DynamoDB metric keys: "
                "attempt=%s delay_seconds=%.2f key_count=%s",
                attempt + 2,
                delay_seconds,
                len(unprocessed_keys.get(table_name, {}).get("Keys", [])),
            )
            sleep(delay_seconds)
            request_items = unprocessed_keys

        raise RuntimeError(
            "DynamoDB BatchGetItem did not process all metric keys after "
            f"{BATCH_GET_MAX_ATTEMPTS} attempts"
        )

    def _merge_metric_delta_item(
        self,
        delta_item: dict[str, Any],
        existing_item: dict[str, Any] | None,
    ) -> dict[str, object]:
        aggregate = metric_aggregate_from_item(existing_item)
        aggregate.merge(metric_delta_from_item(delta_item))

        item = {
            "PK": delta_item["metric_pk"],
            "SK": delta_item["metric_sk"],
            "location_type": delta_item["location_type"],
            "location_code": delta_item["location_code"],
            "location_name": delta_item["location_name"],
            "state_code": delta_item["state_code"],
            "state_name": delta_item["state_name"],
            "region_name": delta_item["region_name"],
            "reference_month": delta_item["target_month"],
            "family_code": delta_item["family_code"],
            "family_title": delta_item["family_title"],
            "admissions": aggregate.admissions,
            "dismissals": aggregate.dismissals,
            "net_balance": aggregate.net_balance,
            "total_turnover": aggregate.total_turnover,
            "salary_sum": aggregate.salary_sum.quantize(MONEY_QUANTIZER),
            "salary_count": aggregate.salary_count,
            "avg_salary": aggregate.avg_salary,
            "GSI1_PK": (
                f"MONTH#{delta_item['target_month']}#PROF#{delta_item['family_code']}"
            ),
            "GSI1_SK": (
                f"NET#{aggregate.net_balance:+012d}"
                f"#LOC#{delta_item['location_type']}{delta_item['location_code']}"
            ),
        }
        return item

    def _download_files(
        self,
        month: ProcessingMonthResult,
        destination: Path,
    ) -> list[LocalCagedFile]:
        local_files: list[LocalCagedFile] = []

        for file_result in month.files:
            source_file = file_result.source_file
            local_path = destination / source_file.filename
            self.logger.info(
                "Downloading CAGED file: filename=%s bucket=%s key=%s destination=%s",
                source_file.filename,
                source_file.s3_bucket,
                source_file.s3_key,
                str(local_path),
            )
            started_at = perf_counter()
            self.s3_client.download_file(
                Bucket=source_file.s3_bucket,
                Key=source_file.s3_key,
                Filename=str(local_path),
                Config=S3_DOWNLOAD_CONFIG,
            )
            elapsed_seconds = perf_counter() - started_at
            self.logger.info(
                "Downloaded CAGED file: filename=%s size_bytes=%s elapsed_seconds=%.2f",
                source_file.filename,
                local_path.stat().st_size,
                elapsed_seconds,
            )

            local_files.append(
                LocalCagedFile(
                    filename=source_file.filename,
                    file_type=source_file.file_type,
                    path=local_path,
                )
            )

        self.logger.debug("Downloaded CAGED files succesfully")

        return local_files


def parse_decimal(value: str) -> Decimal:
    normalized = value.strip().replace(".", "").replace(",", ".")
    if not normalized:
        return Decimal("0")
    try:
        return Decimal(normalized)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value: {value!r}") from exc


def parse_decimal_value(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value: {value!r}") from exc


def metric_aggregate_from_item(item: dict[str, object] | None) -> MetricAggregate:
    if item is None:
        return MetricAggregate()

    return MetricAggregate(
        admissions=int(item.get("admissions", 0)),
        dismissals=int(item.get("dismissals", 0)),
        salary_sum=parse_decimal_value(item.get("salary_sum", "0")),
        salary_count=int(item.get("salary_count", 0)),
    )


def metric_delta_from_item(item: dict[str, object]) -> MetricAggregate:
    return MetricAggregate(
        admissions=int(item["admissions_delta"]),
        dismissals=int(item["dismissals_delta"]),
        salary_sum=parse_decimal_value(item["salary_sum_delta"]),
        salary_count=int(item["salary_count_delta"]),
    )


def is_conditional_check_failure(error: ClientError) -> bool:
    error_code = error.response.get("Error", {}).get("Code")
    return error_code == "ConditionalCheckFailedException"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@contextmanager
def open_caged_csv(path: Path) -> Iterator[TextIO]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            yield file
    except UnicodeDecodeError:
        with path.open("r", encoding="iso-8859-1", newline="") as file:
            yield file
