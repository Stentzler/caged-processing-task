from __future__ import annotations

from dataclasses import dataclass

from app.exceptions import InvalidProcessingJobError

FILE_TYPE_PREFIXES = {
    "CAGEDEXC": "CAGEDEXC",
    "CAGEDFOR": "CAGEDFOR",
    "CAGEDMOV": "CAGEDMOV",
}


@dataclass(frozen=True)
class DownloadedFile:
    status: str
    filename: str
    reference_month: str
    reference_year: str
    s3_bucket: str
    s3_key: str
    s3_uri: str
    size_bytes: int | None = None

    @property
    def file_type(self) -> str:
        for prefix, file_type in FILE_TYPE_PREFIXES.items():
            if self.filename.startswith(prefix):
                return file_type
        raise InvalidProcessingJobError(f"Unsupported CAGED filename: {self.filename}")

    @classmethod
    def from_mapping(cls, value: object, index: int) -> DownloadedFile:
        field_name = f"files[{index}]"
        if not isinstance(value, dict):
            raise InvalidProcessingJobError(f"{field_name} must be an object")

        text_fields = (
            "status",
            "filename",
            "reference_month",
            "reference_year",
            "s3_bucket",
            "s3_key",
            "s3_uri",
        )
        values: dict[str, str] = {}
        for field in text_fields:
            raw_value = value.get(field)
            if not isinstance(raw_value, str) or not raw_value.strip():
                raise InvalidProcessingJobError(
                    f"{field_name}.{field} must be a non-empty string"
                )
            values[field] = raw_value

        size_bytes = value.get("size_bytes")
        if size_bytes is not None and (
            not isinstance(size_bytes, int) or size_bytes < 0
        ):
            raise InvalidProcessingJobError(
                f"{field_name}.size_bytes must be a non-negative integer"
            )

        file = cls(size_bytes=size_bytes, **values)
        if file.reference_month[:4] != file.reference_year:
            raise InvalidProcessingJobError(
                f"{field_name}.reference_year must match reference_month"
            )
        if len(file.reference_month) != 6 or not file.reference_month.isdigit():
            raise InvalidProcessingJobError(
                f"{field_name}.reference_month must be a YYYYMM string"
            )
        if len(file.reference_year) != 4 or not file.reference_year.isdigit():
            raise InvalidProcessingJobError(
                f"{field_name}.reference_year must be a YYYY string"
            )
        expected_uri = f"s3://{file.s3_bucket}/{file.s3_key}"
        if file.s3_uri != expected_uri:
            raise InvalidProcessingJobError(
                f"{field_name}.s3_uri must match s3_bucket and s3_key"
            )
        _ = file.file_type
        return file
