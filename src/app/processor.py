from __future__ import annotations

from typing import Protocol

from app.models import ProcessingMonth, ProcessingResult


class ProcessingEngineProtocol(Protocol):
    def process(self, month: ProcessingMonth) -> ProcessingResult: ...


class NoOpProcessor:
    """Temporary processor used until archive parsing is implemented."""

    def process(self, month: ProcessingMonth) -> ProcessingResult:
        return ProcessingResult(
            status="ok",
            details={
                "processor": "noop",
                "reference_month": month.reference_month,
            },
        )
