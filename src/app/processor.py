from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import TYPE_CHECKING, Any, Protocol, TextIO

from boto3.s3.transfer import TransferConfig

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


class DynamoDBMetricsTableProtocol(Protocol):
    def batch_writer(self) -> DynamoDBBatchWriterProtocol: ...


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
    written_geo_job_metrics: int = 0

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
            "written_geo_job_metrics": self.written_geo_job_metrics,
        }


class CagedProcessor:
    def __init__(
        self,
        s3_client: S3ObjectDownloader,
        geo_job_metrics_table: DynamoDBMetricsTableProtocol,
        cbo_lookup_table: DynamoDBLookupTableProtocol,
        geo_lookup_table: DynamoDBLookupTableProtocol,
        logger: LoggerProtocol,
    ) -> None:
        self.s3_client = s3_client
        self.geo_job_metrics_table = geo_job_metrics_table
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
            details = self._process_files(local_files)

        return ProcessingResult(
            status="ok",
            details={
                "processor": "caged",
                "reference_month": month.reference_month,
                "downloaded_files": len(local_files),
                **details,
            },
        )

    def _process_files(self, files: list[LocalCagedFile]) -> dict[str, Any]:
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

        metric_items = [
            self._build_metric_item(key, aggregate)
            for key, aggregate in aggregates.items()
        ]

        self.logger.info("Writing CAGED metric items: count=%s", len(metric_items))
        self._write_metric_items(metric_items)
        self.logger.info(
            "Finished writing CAGED metric items: count=%s",
            len(metric_items),
        )
        stats.written_geo_job_metrics = len(metric_items)
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
        family_code = cbo_code[:4]
        cached = self._profession_cache.get(family_code)
        if cached is not None:
            return cached

        item = self._get_cbo_lookup_item(family_code)
        if item is None:
            item = self._get_cbo_lookup_item(cbo_code)

        if isinstance(item, dict):
            family_code = str(item.get("family_code") or family_code)
            family_title = str(item.get("family_title") or UNKNOWN_LABEL)
            profession = ProfessionInfo(
                family_code=family_code,
                family_title=family_title,
            )
        else:
            profession = ProfessionInfo(
                family_code=cbo_code[:4],
                family_title=UNKNOWN_LABEL,
            )
            self._warn_missing_cbo_family(cbo_code[:4], cbo_code, stats)

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

    def _write_metric_items(self, items: Iterable[dict[str, object]]) -> None:
        with self.geo_job_metrics_table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)

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


@contextmanager
def open_caged_csv(path: Path) -> Iterator[TextIO]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            yield file
    except UnicodeDecodeError:
        with path.open("r", encoding="iso-8859-1", newline="") as file:
            yield file
