import pytest

from app.exceptions import InvalidProcessingJobError
from app.models import ProcessingJob

VALID_JOB = {
    "status": "COMPLETED",
    "files": [
        {
            "status": "downloaded",
            "filename": "CAGEDEXC202604.7z",
            "reference_month": "202604",
            "reference_year": "2026",
            "s3_bucket": "raw-bucket",
            "s3_key": "raw/caged/202604/CAGEDEXC202604.7z",
            "s3_uri": "s3://raw-bucket/raw/caged/202604/CAGEDEXC202604.7z",
            "size_bytes": 1,
        },
        {
            "status": "downloaded",
            "filename": "CAGEDFOR202604.7z",
            "reference_month": "202604",
            "reference_year": "2026",
            "s3_bucket": "raw-bucket",
            "s3_key": "raw/caged/202604/CAGEDFOR202604.7z",
            "s3_uri": "s3://raw-bucket/raw/caged/202604/CAGEDFOR202604.7z",
            "size_bytes": 2,
        },
        {
            "status": "downloaded",
            "filename": "CAGEDMOV202604.7z",
            "reference_month": "202604",
            "reference_year": "2026",
            "s3_bucket": "raw-bucket",
            "s3_key": "raw/caged/202604/CAGEDMOV202604.7z",
            "s3_uri": "s3://raw-bucket/raw/caged/202604/CAGEDMOV202604.7z",
            "size_bytes": 3,
        },
    ],
}


def test_processing_job_groups_files_by_reference_month() -> None:
    job = ProcessingJob.from_mapping(VALID_JOB)
    months = job.group_by_reference_month()

    assert job.status == "COMPLETED"
    assert len(months) == 1
    assert months[0].reference_month == "202604"
    assert months[0].is_complete is True
    assert sorted(months[0].files_by_type) == ["CAGEDEXC", "CAGEDFOR", "CAGEDMOV"]


def test_processing_job_reports_missing_required_file_type() -> None:
    event = {
        **VALID_JOB,
        "files": VALID_JOB["files"][:2],
    }
    job = ProcessingJob.from_mapping(event)
    month = job.group_by_reference_month()[0]

    assert month.is_complete is False
    assert month.missing_file_types == ["CAGEDMOV"]


def test_processing_job_rejects_invalid_s3_reference() -> None:
    event = {
        **VALID_JOB,
        "files": [
            *VALID_JOB["files"][:2],
            {
                **VALID_JOB["files"][2],
                "s3_bucket": "",
            },
        ],
    }

    with pytest.raises(InvalidProcessingJobError, match=r"files\[2\].s3_bucket"):
        ProcessingJob.from_mapping(event)


def test_processing_job_rejects_duplicate_file_type_per_month() -> None:
    event = {
        **VALID_JOB,
        "files": [
            *VALID_JOB["files"],
            {
                "status": "downloaded",
                "filename": "CAGEDMOV202604-copy.7z",
                "reference_month": "202604",
                "reference_year": "2026",
                "s3_bucket": "raw-bucket",
                "s3_key": "raw/caged/202604/CAGEDMOV202604-copy.7z",
                "s3_uri": "s3://raw-bucket/raw/caged/202604/CAGEDMOV202604-copy.7z",
                "size_bytes": 4,
            },
        ],
    }

    with pytest.raises(InvalidProcessingJobError, match="Duplicate file type"):
        ProcessingJob.from_mapping(event).group_by_reference_month()
