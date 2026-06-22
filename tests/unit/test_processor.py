from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.models import ProcessingJob, ProcessingResult
from app.processor import CagedProcessor, LocalCagedFile
from app.service import ProcessingFileResult, ProcessingMonthResult
from tests.unit.test_models import VALID_JOB

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SAMPLE_DIR = PROJECT_ROOT / "sample"


@dataclass
class FakeS3Downloader:
    calls: list[dict[str, object]] = field(default_factory=list)

    def download_file(
        self,
        *,
        Bucket: str,
        Key: str,
        Filename: str,
        Config: object | None = None,
    ) -> None:
        self.calls.append(
            {
                "Bucket": Bucket,
                "Key": Key,
                "Filename": Filename,
                "Config": Config,
            }
        )
        filename = Path(Filename).name
        if filename.startswith("CAGEDEXC"):
            shutil.copyfile(SAMPLE_DIR / "CAGEDEXC.csv", Filename)
        elif filename.startswith("CAGEDFOR"):
            shutil.copyfile(SAMPLE_DIR / "CAGEDFOR.csv", Filename)
        elif filename.startswith("CAGEDMOV"):
            shutil.copyfile(SAMPLE_DIR / "CAGEDMOV.csv", Filename)


@dataclass
class FakeMetricsTable:
    put_calls: list[dict[str, Any]] = field(default_factory=list)

    def batch_writer(self) -> FakeMetricsTable:
        return self

    def __enter__(self) -> FakeMetricsTable:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def put_item(self, **kwargs: object) -> dict[str, Any]:
        self.put_calls.append(kwargs)
        return {}


@dataclass
class FakeCboLookupTable:
    missing_codes: set[str] = field(default_factory=set)
    get_calls: list[str] = field(default_factory=list)

    def get_item(self, **kwargs: object) -> dict[str, Any]:
        code = str(kwargs["Key"]["code"])
        self.get_calls.append(code)
        if code in self.missing_codes:
            return {}
        return {
            "Item": {
                "code": code,
                "family_code": code[:4],
                "family_title": f"Family {code[:4]}",
            }
        }


@dataclass
class FakeGeoLookupTable:
    missing_codes: set[str] = field(default_factory=set)
    get_calls: list[dict[str, str]] = field(default_factory=list)

    def get_item(self, **kwargs: object) -> dict[str, Any]:
        code = str(kwargs["Key"]["code"])
        location_type = str(kwargs["Key"]["type"])
        self.get_calls.append({"code": code, "type": location_type})
        if code in self.missing_codes:
            return {}
        if location_type == "STATE":
            return {
                "Item": {
                    "code": code,
                    "type": "STATE",
                    "name": f"State {code}",
                    "state_code": code,
                    "state_name": f"State {code}",
                    "region_name": "Region",
                }
            }
        return {
            "Item": {
                "code": code,
                "type": "CITY",
                "name": f"City {code}",
                "state_code": code[:2],
                "state_name": f"State {code[:2]}",
                "region_name": "Region",
            }
        }


@dataclass
class FakeLogger:
    messages: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def info(self, message: str, *args: object, **kwargs: object) -> None:
        self.messages.append(message % args)

    def warning(self, message: str, *args: object, **kwargs: object) -> None:
        self.warnings.append(message % args)

    def debug(self, message: str, *args: object, **kwargs: object) -> None:
        self.messages.append(message % args)


def test_caged_processor_returns_ok_result() -> None:
    job = ProcessingJob.from_mapping(csv_job())
    month = job.group_by_reference_month()[0]
    month_result = ProcessingMonthResult(
        reference_month=month.reference_month,
        reference_year=month.reference_year,
        processing_status="processing",
        missing_file_types=[],
        files=[
            ProcessingFileResult(
                source_file=file,
                filename=file.filename,
                file_type=file.file_type,
                process_id=f"process-{index}",
                processing_status="processing",
            )
            for index, file in enumerate(month.files, start=1)
        ],
        processor_result=ProcessingResult(status="", details={}),
    )
    s3_client = FakeS3Downloader()
    metrics_table = FakeMetricsTable()
    logger = FakeLogger()
    processor = CagedProcessor(
        s3_client=s3_client,
        geo_job_metrics_table=metrics_table,
        cbo_lookup_table=FakeCboLookupTable(),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=logger,
    )

    result = processor.process(month_result)

    assert result.status == "ok"
    assert result.details["processor"] == "caged"
    assert result.details["reference_month"] == "202604"
    assert result.details["downloaded_files"] == 3
    assert result.details["parsed_rows_by_file_type"] == {
        "CAGEDMOV": 9,
        "CAGEDFOR": 9,
        "CAGEDEXC": 9,
    }
    assert result.details["written_geo_job_metrics"] > 0
    assert len(s3_client.calls) == 3
    assert s3_client.calls[0]["Bucket"] == "raw-bucket"
    assert s3_client.calls[0]["Key"] == "raw/caged/202604/CAGEDEXC202604.csv"
    assert s3_client.calls[0]["Filename"].endswith("CAGEDEXC202604.csv")
    assert s3_client.calls[0]["Config"] is not None
    assert logger.messages[0].startswith(
        "Downloading CAGED file: filename=CAGEDEXC202604.csv"
    )
    assert (
        metric_item(metrics_table, "LOC#CITY#351905#MONTH#202604", "PROF#6220")[
            "family_title"
        ]
        == "Family 6220"
    )
    assert (
        metric_item(metrics_table, "LOC#STATE#35#MONTH#202604", "PROF#ALL")[
            "family_title"
        ]
        == "All professions"
    )


def test_process_files_writes_city_state_family_and_total_metrics() -> None:
    metrics_table = FakeMetricsTable()
    cbo_lookup_table = FakeCboLookupTable()
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=metrics_table,
        cbo_lookup_table=cbo_lookup_table,
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )

    details = processor._process_files(sample_local_files())

    city_family = metric_item(
        metrics_table,
        "LOC#CITY#351905#MONTH#202604",
        "PROF#6220",
    )
    assert city_family["admissions"] == 1
    assert city_family["dismissals"] == 0
    assert city_family["net_balance"] == 1
    assert city_family["salary_count"] == 1
    assert city_family["avg_salary"].to_eng_string() == "1654.62"

    city_total = metric_item(
        metrics_table,
        "LOC#CITY#351905#MONTH#202604",
        "PROF#ALL",
    )
    assert city_total["admissions"] == 1
    assert city_total["family_code"] == "ALL"

    state_total = metric_item(
        metrics_table,
        "LOC#STATE#35#MONTH#202604",
        "PROF#ALL",
    )
    assert state_total["admissions"] == 6
    assert details["written_geo_job_metrics"] == len(metrics_table.put_calls)
    assert "6220" in cbo_lookup_table.get_calls
    assert "622020" not in cbo_lookup_table.get_calls


def test_process_files_accepts_txt_caged_data_files(tmp_path: Path) -> None:
    txt_path = tmp_path / "CAGEDMOV.txt"
    txt_path.write_text((SAMPLE_DIR / "CAGEDMOV.csv").read_text())
    metrics_table = FakeMetricsTable()
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=metrics_table,
        cbo_lookup_table=FakeCboLookupTable(),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )

    details = processor._process_files(
        [
            LocalCagedFile("CAGEDEXC.csv", "CAGEDEXC", SAMPLE_DIR / "CAGEDEXC.csv"),
            LocalCagedFile("CAGEDFOR.csv", "CAGEDFOR", SAMPLE_DIR / "CAGEDFOR.csv"),
            LocalCagedFile("CAGEDMOV.txt", "CAGEDMOV", txt_path),
        ]
    )

    assert details["parsed_rows_by_file_type"] == {
        "CAGEDMOV": 9,
        "CAGEDFOR": 9,
        "CAGEDEXC": 9,
    }
    assert (
        metric_item(
            metrics_table,
            "LOC#CITY#351905#MONTH#202604",
            "PROF#6220",
        )["admissions"]
        == 1
    )


def test_process_files_applies_exclusion_as_inverse_delta() -> None:
    metrics_table = FakeMetricsTable()
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=metrics_table,
        cbo_lookup_table=FakeCboLookupTable(),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )

    processor._process_files(sample_local_files())

    exclusion_metric = metric_item(
        metrics_table,
        "LOC#CITY#210140#MONTH#202001",
        "PROF#7825",
    )
    assert exclusion_metric["admissions"] == -1
    assert exclusion_metric["dismissals"] == 0
    assert exclusion_metric["net_balance"] == -1
    assert exclusion_metric["salary_count"] == -1
    assert exclusion_metric["avg_salary"].to_eng_string() == "1200.00"


def test_process_files_warns_and_falls_back_when_cbo_lookup_is_missing() -> None:
    metrics_table = FakeMetricsTable()
    logger = FakeLogger()
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=metrics_table,
        cbo_lookup_table=FakeCboLookupTable(missing_codes={"6220", "622020"}),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=logger,
    )

    details = processor._process_files(sample_local_files())

    item = metric_item(metrics_table, "LOC#CITY#351905#MONTH#202604", "PROF#6220")
    assert item["family_title"] == "UNKNOWN"
    assert details["missing_cbo_lookup_count"] == 1
    assert logger.warnings == [
        "Missing CBO family lookup: family_code=6220 cbo_code=622020"
    ]


def csv_job() -> dict[str, object]:
    files = []
    for file in VALID_JOB["files"]:
        filename = file["filename"].replace(".7z", ".csv")
        files.append(
            {
                **file,
                "filename": filename,
                "s3_key": file["s3_key"].replace(".7z", ".csv"),
                "s3_uri": file["s3_uri"].replace(".7z", ".csv"),
            }
        )
    return {**VALID_JOB, "files": files}


def sample_local_files() -> list[LocalCagedFile]:
    return [
        LocalCagedFile("CAGEDEXC.csv", "CAGEDEXC", SAMPLE_DIR / "CAGEDEXC.csv"),
        LocalCagedFile("CAGEDFOR.csv", "CAGEDFOR", SAMPLE_DIR / "CAGEDFOR.csv"),
        LocalCagedFile("CAGEDMOV.csv", "CAGEDMOV", SAMPLE_DIR / "CAGEDMOV.csv"),
    ]


def metric_item(
    table: FakeMetricsTable,
    pk: str,
    sk: str,
) -> dict[str, Any]:
    for call in table.put_calls:
        item = call["Item"]
        if item["PK"] == pk and item["SK"] == sk:
            return item
    raise AssertionError(f"Metric item not found: PK={pk} SK={sk}")
