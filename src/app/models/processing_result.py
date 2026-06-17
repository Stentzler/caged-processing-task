from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ProcessingResult:
    status: str
    details: dict[str, Any]
