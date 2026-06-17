from __future__ import annotations

from dataclasses import dataclass

from app.models.downloaded_file import DownloadedFile

REQUIRED_FILE_TYPES = frozenset({"CAGEDMOV", "CAGEDFOR", "CAGEDEXC"})


@dataclass(frozen=True)
class ProcessingMonth:
    reference_month: str
    reference_year: str
    files_by_type: dict[str, DownloadedFile]

    @property
    def files(self) -> list[DownloadedFile]:
        return [
            self.files_by_type[file_type] for file_type in sorted(self.files_by_type)
        ]

    @property
    def missing_file_types(self) -> list[str]:
        return sorted(REQUIRED_FILE_TYPES - set(self.files_by_type))

    @property
    def is_complete(self) -> bool:
        return not self.missing_file_types
