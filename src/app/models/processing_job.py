from __future__ import annotations

from dataclasses import dataclass
from itertools import groupby

from app.exceptions import InvalidProcessingJobError
from app.models.downloaded_file import DownloadedFile
from app.models.processing_month import ProcessingMonth


@dataclass(frozen=True)
class ProcessingJob:
    status: str
    files: list[DownloadedFile]

    @classmethod
    def from_mapping(cls, value: object) -> ProcessingJob:
        if not isinstance(value, dict):
            raise InvalidProcessingJobError("Processing job must be a JSON object")

        status = value.get("status")
        if not isinstance(status, str) or not status.strip():
            raise InvalidProcessingJobError("status must be a string")

        raw_files = value.get("files")
        if not isinstance(raw_files, list) or not raw_files:
            raise InvalidProcessingJobError("files must be a non-empty array")

        files = [
            DownloadedFile.from_mapping(raw_file, index)
            for index, raw_file in enumerate(raw_files)
        ]

        return cls(status=status, files=files)

    def group_by_reference_month(self) -> list[ProcessingMonth]:
        sorted_files = sorted(
            self.files,
            key=lambda file: (file.reference_month, file.file_type, file.filename),
        )
        groups: list[ProcessingMonth] = []
        for reference_month, group in groupby(
            sorted_files,
            key=lambda file: file.reference_month,
        ):
            month_files = list(group)
            reference_year = month_files[0].reference_year
            files_by_type: dict[str, DownloadedFile] = {}
            for file in month_files:
                existing_file = files_by_type.get(file.file_type)
                if existing_file is not None:
                    raise InvalidProcessingJobError(
                        "Duplicate file type for reference_month "
                        f"{reference_month}: {file.file_type}"
                    )
                files_by_type[file.file_type] = file

            groups.append(
                ProcessingMonth(
                    reference_month=reference_month,
                    reference_year=reference_year,
                    files_by_type=files_by_type,
                )
            )
        return groups
