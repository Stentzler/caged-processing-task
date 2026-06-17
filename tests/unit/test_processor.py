from __future__ import annotations

from app.models import ProcessingJob
from app.processor import NoOpProcessor
from tests.unit.test_models import VALID_JOB


def test_noop_processor_returns_ok_result() -> None:
    job = ProcessingJob.from_mapping(VALID_JOB)
    month = job.group_by_reference_month()[0]
    processor = NoOpProcessor()

    result = processor.process(month)

    assert result.status == "ok"
    assert result.details == {
        "processor": "noop",
        "reference_month": "202604",
    }
