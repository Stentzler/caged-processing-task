from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from app.models import ProcessingJob, ProcessingResult
from app.processor import CagedProcessor, LocalCagedFile, ProcessingStats
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
class FakeTableMeta:
    client: FakeDynamoClient


@dataclass
class FakeDynamoClient:
    tables: dict[str, Any] = field(default_factory=dict)
    batch_get_calls: list[dict[str, Any]] = field(default_factory=list)
    transact_write_calls: list[dict[str, Any]] = field(default_factory=list)
    return_unprocessed_keys_once: bool = False

    def register(self, table: Any) -> None:
        self.tables[table.name] = table

    def batch_get_item(self, **kwargs: object) -> dict[str, Any]:
        self.batch_get_calls.append(kwargs)
        request_items = kwargs["RequestItems"]
        responses: dict[str, list[dict[str, Any]]] = {}
        unprocessed_keys: dict[str, dict[str, Any]] = {}

        for table_name, request in request_items.items():
            keys = request["Keys"]
            processed_keys = keys
            if self.return_unprocessed_keys_once:
                processed_keys = keys[:-1]
                unprocessed_keys[table_name] = {
                    **request,
                    "Keys": keys[-1:],
                }
                self.return_unprocessed_keys_once = False

            table = self.tables[table_name]
            responses[table_name] = [
                table.items[(key["PK"], key["SK"])]
                for key in processed_keys
                if (key["PK"], key["SK"]) in table.items
            ]

        return {
            "Responses": responses,
            "UnprocessedKeys": unprocessed_keys,
        }

    def transact_write_items(self, **kwargs: object) -> dict[str, Any]:
        self.transact_write_calls.append(kwargs)
        transact_items = kwargs["TransactItems"]

        for operation in transact_items:
            update = operation.get("Update")
            if update is None:
                continue
            table = self.tables[update["TableName"]]
            key = update["Key"]
            item = table.items[(key["PK"], key["SK"])]
            values = update["ExpressionAttributeValues"]
            if item["status"] != values[":pending"]:
                raise ClientError(
                    {
                        "Error": {
                            "Code": "TransactionCanceledException",
                            "Message": "condition failed",
                        }
                    },
                    "TransactWriteItems",
                )

        for operation in transact_items:
            put = operation.get("Put")
            if put is not None:
                table = self.tables[put["TableName"]]
                item = put["Item"]
                table.items[(item["PK"], item["SK"])] = item
                table.direct_put_calls.append({"Item": item})
                continue

            update = operation["Update"]
            table = self.tables[update["TableName"]]
            key = update["Key"]
            item = table.items[(key["PK"], key["SK"])]
            values = update["ExpressionAttributeValues"]
            item["status"] = values[":status"]
            item["updated_at"] = values[":now"]

        return {}


FAKE_DYNAMO_CLIENT = FakeDynamoClient()


def shared_fake_dynamo_client() -> FakeDynamoClient:
    return FAKE_DYNAMO_CLIENT


@dataclass
class FakeMetricsTable:
    name: str = "caged_geo_job_metrics"
    client: FakeDynamoClient = field(default_factory=shared_fake_dynamo_client)
    items: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    put_calls: list[dict[str, Any]] = field(default_factory=list)
    batch_put_calls: list[dict[str, Any]] = field(default_factory=list)
    direct_put_calls: list[dict[str, Any]] = field(default_factory=list)
    _is_batch_writer: bool = False

    def __post_init__(self) -> None:
        self.client.register(self)

    @property
    def meta(self) -> FakeTableMeta:
        return FakeTableMeta(client=self.client)

    def batch_writer(self) -> FakeMetricsTable:
        return self

    def __enter__(self) -> FakeMetricsTable:
        self._is_batch_writer = True
        return self

    def __exit__(self, *args: object) -> None:
        self._is_batch_writer = False
        return None

    def put_item(self, **kwargs: object) -> dict[str, Any]:
        self.put_calls.append(kwargs)
        item = kwargs["Item"]
        self.items[(item["PK"], item["SK"])] = item
        if self._is_batch_writer:
            self.batch_put_calls.append(kwargs)
        else:
            self.direct_put_calls.append(kwargs)
        return {}

    def get_item(self, **kwargs: object) -> dict[str, Any]:
        key = kwargs["Key"]
        item = self.items.get((key["PK"], key["SK"]))
        if item is None:
            return {}
        return {"Item": item}


@dataclass
class FakeRevisionTable:
    name: str = "caged_metric_revisions"
    client: FakeDynamoClient = field(default_factory=shared_fake_dynamo_client)
    items: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    put_calls: list[dict[str, Any]] = field(default_factory=list)
    update_calls: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.client.register(self)

    @property
    def meta(self) -> FakeTableMeta:
        return FakeTableMeta(client=self.client)

    def put_item(self, **kwargs: object) -> dict[str, Any]:
        item = kwargs["Item"]
        key = (item["PK"], item["SK"])
        if kwargs.get("ConditionExpression") and key in self.items:
            raise ClientError(
                {
                    "Error": {
                        "Code": "ConditionalCheckFailedException",
                        "Message": "already exists",
                    }
                },
                "PutItem",
            )
        self.put_calls.append(kwargs)
        self.items[key] = item
        return {}

    def query(self, **kwargs: object) -> dict[str, Any]:
        pk = kwargs["ExpressionAttributeValues"][":pk"]
        return {
            "Items": [
                item for (item_pk, _), item in self.items.items() if item_pk == pk
            ]
        }

    def update_item(self, **kwargs: object) -> dict[str, Any]:
        self.update_calls.append(kwargs)
        key = kwargs["Key"]
        item = self.items[(key["PK"], key["SK"])]
        values = kwargs["ExpressionAttributeValues"]
        item["status"] = values[":status"]
        item["updated_at"] = values[":now"]
        return {}


def fake_metric_batches_table() -> FakeRevisionTable:
    return FakeRevisionTable(name="caged_metric_batches")


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
        metric_batches_table=fake_metric_batches_table(),
        metric_revisions_table=FakeRevisionTable(),
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
    assert result.details["new_metric_batches"] > 0
    assert result.details["applied_metric_batches"] > 0
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
        metric_batches_table=fake_metric_batches_table(),
        metric_revisions_table=FakeRevisionTable(),
        cbo_lookup_table=cbo_lookup_table,
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )

    details = processor._process_files(sample_local_files(), "202604")

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
    assert details["new_metric_batches"] == details["applied_metric_batches"]
    assert "6220" in cbo_lookup_table.get_calls
    assert "622020" not in cbo_lookup_table.get_calls


def test_process_files_accepts_txt_caged_data_files(tmp_path: Path) -> None:
    txt_path = tmp_path / "CAGEDMOV.txt"
    txt_path.write_text((SAMPLE_DIR / "CAGEDMOV.csv").read_text())
    metrics_table = FakeMetricsTable()
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=metrics_table,
        metric_batches_table=fake_metric_batches_table(),
        metric_revisions_table=FakeRevisionTable(),
        cbo_lookup_table=FakeCboLookupTable(),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )

    details = processor._process_files(
        [
            LocalCagedFile("CAGEDEXC.csv", "CAGEDEXC", SAMPLE_DIR / "CAGEDEXC.csv"),
            LocalCagedFile("CAGEDFOR.csv", "CAGEDFOR", SAMPLE_DIR / "CAGEDFOR.csv"),
            LocalCagedFile("CAGEDMOV.txt", "CAGEDMOV", txt_path),
        ],
        "202604",
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
        metric_batches_table=fake_metric_batches_table(),
        metric_revisions_table=FakeRevisionTable(),
        cbo_lookup_table=FakeCboLookupTable(),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )

    processor._process_files(sample_local_files(), "202604")

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


def test_process_files_does_not_apply_existing_revisions_twice() -> None:
    metrics_table = FakeMetricsTable()
    revision_table = FakeRevisionTable()
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=metrics_table,
        metric_batches_table=fake_metric_batches_table(),
        metric_revisions_table=revision_table,
        cbo_lookup_table=FakeCboLookupTable(),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )

    first_details = processor._process_files(sample_local_files(), "202604")
    first_exclusion_metric = metric_item(
        metrics_table,
        "LOC#CITY#210140#MONTH#202001",
        "PROF#7825",
    )

    second_details = processor._process_files(sample_local_files(), "202604")
    second_exclusion_metric = metric_item(
        metrics_table,
        "LOC#CITY#210140#MONTH#202001",
        "PROF#7825",
    )

    assert first_details["new_metric_revisions"] > 0
    assert first_details["applied_metric_revisions"] > 0
    assert second_details["new_metric_revisions"] == 0
    assert second_details["skipped_metric_revisions"] > 0
    assert second_details["applied_metric_revisions"] == 0
    assert first_exclusion_metric["admissions"] == -1
    assert second_exclusion_metric["admissions"] == -1


def test_apply_metric_delta_uses_native_values_with_resource_client() -> None:
    metrics_table = FakeMetricsTable()
    metric_batches_table = fake_metric_batches_table()
    metric_revisions_table = FakeRevisionTable()
    delta_item = {
        "PK": "BATCH#202604",
        "SK": "METRIC#LOC#CITY#110001#MONTH#202604#PROF#1231",
        "metric_pk": "LOC#CITY#110001#MONTH#202604",
        "metric_sk": "PROF#1231",
        "target_month": "202604",
        "location_type": "CITY",
        "location_code": "110001",
        "location_name": "Alta Floresta D'Oeste",
        "state_code": "11",
        "state_name": "Rondônia",
        "region_name": "Norte",
        "family_code": "1231",
        "family_title": "Diretores administrativos e financeiros",
        "admissions_delta": Decimal("1"),
        "dismissals_delta": Decimal("0"),
        "salary_sum_delta": Decimal("1621"),
        "salary_count_delta": Decimal("1"),
        "status": "pending",
    }
    metric_batches_table.items[(delta_item["PK"], delta_item["SK"])] = delta_item
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=metrics_table,
        metric_batches_table=metric_batches_table,
        metric_revisions_table=metric_revisions_table,
        cbo_lookup_table=FakeCboLookupTable(),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )

    processor._apply_metric_delta_item(
        source_table=metric_batches_table,
        delta_item=delta_item,
    )

    transaction = metrics_table.client.transact_write_calls[-1]["TransactItems"]
    assert transaction[0]["Put"]["Item"]["PK"] == delta_item["metric_pk"]
    assert transaction[0]["Put"]["Item"]["salary_sum"] == Decimal("1621.00")
    assert transaction[1]["Update"]["Key"]["PK"] == delta_item["PK"]
    assert transaction[1]["Update"]["ExpressionAttributeValues"][":status"] == (
        "applied"
    )
    assert delta_item["status"] == "applied"


def test_apply_pending_metric_deltas_batches_reads_and_transactions() -> None:
    client = FakeDynamoClient()
    metrics_table = FakeMetricsTable(client=client)
    metric_batches_table = FakeRevisionTable(
        name="caged_metric_batches",
        client=client,
    )
    metric_revisions_table = FakeRevisionTable(client=client)
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=metrics_table,
        metric_batches_table=metric_batches_table,
        metric_revisions_table=metric_revisions_table,
        cbo_lookup_table=FakeCboLookupTable(),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )

    for index in range(51):
        delta_item = metric_delta_item(index)
        metric_batches_table.items[(delta_item["PK"], delta_item["SK"])] = delta_item

    existing_delta = metric_delta_item(0)
    metrics_table.items[(existing_delta["metric_pk"], existing_delta["metric_sk"])] = {
        "PK": existing_delta["metric_pk"],
        "SK": existing_delta["metric_sk"],
        "admissions": 1,
        "dismissals": 0,
        "salary_sum": Decimal("1000"),
        "salary_count": 1,
    }

    applied_count, merged_count = processor._apply_pending_metric_delta_items(
        source_table=metric_batches_table,
        partition_key="BATCH#202604",
    )

    assert applied_count == 51
    assert merged_count == 1
    assert len(client.batch_get_calls) == 2
    assert len(client.transact_write_calls) == 2
    assert len(client.transact_write_calls[0]["TransactItems"]) == 100
    assert len(client.transact_write_calls[1]["TransactItems"]) == 2
    assert all(
        item["status"] == "applied" for item in metric_batches_table.items.values()
    )


def test_batch_get_metric_items_retries_unprocessed_keys() -> None:
    client = FakeDynamoClient(return_unprocessed_keys_once=True)
    metrics_table = FakeMetricsTable(client=client)
    metric_batches_table = FakeRevisionTable(
        name="caged_metric_batches",
        client=client,
    )
    metric_revisions_table = FakeRevisionTable(client=client)
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=metrics_table,
        metric_batches_table=metric_batches_table,
        metric_revisions_table=metric_revisions_table,
        cbo_lookup_table=FakeCboLookupTable(),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )
    delta_items = (metric_delta_item(0), metric_delta_item(1))
    for delta_item in delta_items:
        metrics_table.items[(delta_item["metric_pk"], delta_item["metric_sk"])] = {
            "PK": delta_item["metric_pk"],
            "SK": delta_item["metric_sk"],
        }

    existing_items = processor._batch_get_metric_items(delta_items)

    assert len(existing_items) == 2
    assert len(client.batch_get_calls) == 2


def test_cross_month_fixtures_produce_order_independent_metrics() -> None:
    first_order = process_order_independence_months(("202601", "202602"))
    second_order = process_order_independence_months(("202602", "202601"))

    assert first_order == second_order
    assert len(first_order) == 24
    assert_metric_values(
        first_order,
        month="202601",
        city_code="355030",
        admissions=1,
        dismissals=1,
        salary_sum="2000.00",
        salary_count=1,
    )
    assert_metric_values(
        first_order,
        month="202601",
        city_code="420540",
        admissions=2,
        dismissals=0,
        salary_sum="3000.00",
        salary_count=2,
    )
    assert_metric_values(
        first_order,
        month="202601",
        city_code="270840",
        admissions=1,
        dismissals=0,
        salary_sum="2000.00",
        salary_count=1,
    )
    assert_metric_values(
        first_order,
        month="202602",
        city_code="355030",
        admissions=1,
        dismissals=1,
        salary_sum="3000.00",
        salary_count=1,
    )
    assert_metric_values(
        first_order,
        month="202602",
        city_code="420540",
        admissions=2,
        dismissals=0,
        salary_sum="4500.00",
        salary_count=2,
    )
    assert_metric_values(
        first_order,
        month="202602",
        city_code="270840",
        admissions=1,
        dismissals=0,
        salary_sum="3000.00",
        salary_count=1,
    )


def test_process_files_merges_current_metrics_with_existing_items() -> None:
    metrics_table = FakeMetricsTable()
    metrics_table.items[("LOC#CITY#351905#MONTH#202604", "PROF#6220")] = {
        "PK": "LOC#CITY#351905#MONTH#202604",
        "SK": "PROF#6220",
        "admissions": 2,
        "dismissals": 0,
        "salary_sum": "2000.00",
        "salary_count": 2,
    }
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=metrics_table,
        metric_batches_table=fake_metric_batches_table(),
        metric_revisions_table=FakeRevisionTable(),
        cbo_lookup_table=FakeCboLookupTable(),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )

    details = processor._process_files(sample_local_files(), "202604")

    city_family = metric_item(
        metrics_table,
        "LOC#CITY#351905#MONTH#202604",
        "PROF#6220",
    )
    assert city_family["admissions"] == 3
    assert city_family["salary_count"] == 3
    assert city_family["salary_sum"].to_eng_string() == "3654.62"
    assert city_family["avg_salary"].to_eng_string() == "1218.21"
    assert city_family["GSI1_SK"].startswith("NET#+00000000003")
    assert details["merged_metric_batches"] == 1


def test_process_files_does_not_apply_existing_metric_batches_twice() -> None:
    metrics_table = FakeMetricsTable()
    metric_batches_table = fake_metric_batches_table()
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=metrics_table,
        metric_batches_table=metric_batches_table,
        metric_revisions_table=FakeRevisionTable(),
        cbo_lookup_table=FakeCboLookupTable(),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )

    first_details = processor._process_files(sample_local_files(), "202604")
    first_city_family = metric_item(
        metrics_table,
        "LOC#CITY#351905#MONTH#202604",
        "PROF#6220",
    )

    second_details = processor._process_files(sample_local_files(), "202604")

    second_city_family = metric_item(
        metrics_table,
        "LOC#CITY#351905#MONTH#202604",
        "PROF#6220",
    )
    assert first_details["new_metric_batches"] > 0
    assert first_details["applied_metric_batches"] > 0
    assert second_details["new_metric_batches"] == 0
    assert second_details["skipped_metric_batches"] > 0
    assert second_details["applied_metric_batches"] == 0
    assert first_city_family["admissions"] == 1
    assert second_city_family["admissions"] == 1


def test_process_files_warns_and_falls_back_when_cbo_lookup_is_missing() -> None:
    metrics_table = FakeMetricsTable()
    logger = FakeLogger()
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=metrics_table,
        metric_batches_table=fake_metric_batches_table(),
        metric_revisions_table=FakeRevisionTable(),
        cbo_lookup_table=FakeCboLookupTable(missing_codes={"6220", "622020"}),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=logger,
    )

    details = processor._process_files(sample_local_files(), "202604")

    item = metric_item(metrics_table, "LOC#CITY#351905#MONTH#202604", "PROF#6220")
    assert item["family_title"] == "UNKNOWN"
    assert details["missing_cbo_lookup_count"] == 1
    assert logger.warnings == [
        "Missing CBO family lookup: family_code=6220 cbo_code=622020"
    ]


def test_get_profession_normalizes_five_digit_cbo_codes() -> None:
    cbo_lookup_table = FakeCboLookupTable()
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=FakeMetricsTable(),
        metric_batches_table=fake_metric_batches_table(),
        metric_revisions_table=FakeRevisionTable(),
        cbo_lookup_table=cbo_lookup_table,
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )

    profession = processor._get_profession("10105", stats=ProcessingStats.empty())

    assert profession.family_code == "0101"
    assert profession.family_title == "Family 0101"
    assert cbo_lookup_table.get_calls == ["0101"]


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


def order_independence_local_files(month: str) -> list[LocalCagedFile]:
    return [
        LocalCagedFile(
            f"CAGEDEXC{month}.txt",
            "CAGEDEXC",
            SAMPLE_DIR / "caged_files" / f"CAGEDEXC{month}.txt",
        ),
        LocalCagedFile(
            f"CAGEDFOR{month}.txt",
            "CAGEDFOR",
            SAMPLE_DIR / "caged_files" / f"CAGEDFOR{month}.txt",
        ),
        LocalCagedFile(
            f"CAGEDMOV{month}.txt",
            "CAGEDMOV",
            SAMPLE_DIR / "caged_files" / f"CAGEDMOV{month}.txt",
        ),
    ]


def process_order_independence_months(
    months: tuple[str, str],
) -> dict[tuple[str, str], dict[str, Any]]:
    client = FakeDynamoClient()
    metrics_table = FakeMetricsTable(client=client)
    metric_batches_table = FakeRevisionTable(
        name="caged_metric_batches",
        client=client,
    )
    metric_revisions_table = FakeRevisionTable(client=client)
    processor = CagedProcessor(
        s3_client=FakeS3Downloader(),
        geo_job_metrics_table=metrics_table,
        metric_batches_table=metric_batches_table,
        metric_revisions_table=metric_revisions_table,
        cbo_lookup_table=FakeCboLookupTable(),
        geo_lookup_table=FakeGeoLookupTable(),
        logger=FakeLogger(),
    )

    for month in months:
        processor._process_files(order_independence_local_files(month), month)

    assert len(metric_batches_table.items) == 24
    assert len(metric_revisions_table.items) == 24
    assert all(
        item["status"] == "applied" for item in metric_batches_table.items.values()
    )
    assert all(
        item["status"] == "applied" for item in metric_revisions_table.items.values()
    )
    return metrics_table.items


def assert_metric_values(
    items: dict[tuple[str, str], dict[str, Any]],
    *,
    month: str,
    city_code: str,
    admissions: int,
    dismissals: int,
    salary_sum: str,
    salary_count: int,
) -> None:
    item = items[(f"LOC#CITY#{city_code}#MONTH#{month}", "PROF#6220")]
    assert item["admissions"] == admissions
    assert item["dismissals"] == dismissals
    assert item["salary_sum"] == Decimal(salary_sum)
    assert item["salary_count"] == salary_count


def metric_delta_item(index: int) -> dict[str, Any]:
    metric_pk = f"LOC#CITY#{index:06d}#MONTH#202604"
    metric_sk = "PROF#1231"
    return {
        "PK": "BATCH#202604",
        "SK": f"METRIC#{metric_pk}#{metric_sk}",
        "metric_pk": metric_pk,
        "metric_sk": metric_sk,
        "target_month": "202604",
        "location_type": "CITY",
        "location_code": f"{index:06d}",
        "location_name": f"City {index}",
        "state_code": "11",
        "state_name": "Rondônia",
        "region_name": "Norte",
        "family_code": "1231",
        "family_title": "Diretores administrativos e financeiros",
        "admissions_delta": Decimal("1"),
        "dismissals_delta": Decimal("0"),
        "salary_sum_delta": Decimal("1621"),
        "salary_count_delta": Decimal("1"),
        "status": "pending",
    }


def metric_item(
    table: FakeMetricsTable,
    pk: str,
    sk: str,
) -> dict[str, Any]:
    item = table.items.get((pk, sk))
    if item is not None:
        return item
    raise AssertionError(f"Metric item not found: PK={pk} SK={sk}")
