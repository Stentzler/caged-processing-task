import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    """Runtime configuration loaded from environment variables."""

    REGISTRY_TABLE_NAME: str = field(
        default_factory=lambda: os.getenv(
            "REGISTRY_TABLE_NAME",
            "downloaded_files_registry",
        )
    )
    REGISTRY_ID: str = field(
        default_factory=lambda: os.getenv("REGISTRY_ID", "ftp_tree")
    )
    PROCESS_AUDIT_TABLE_NAME: str = field(
        default_factory=lambda: os.getenv(
            "PROCESS_AUDIT_TABLE_NAME",
            "caged_processes",
        )
    )
    PROCESSING_JOB_JSON: str = field(
        default_factory=lambda: os.getenv("PROCESSING_JOB_JSON", "")
    )
    PROCESSING_JOB_S3_URI: str = field(
        default_factory=lambda: os.getenv("PROCESSING_JOB_S3_URI", "")
    )
    AWS_REGION: str | None = field(default_factory=lambda: os.getenv("AWS_REGION"))
    LOG_LEVEL: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    def __post_init__(self) -> None:
        if not self.REGISTRY_TABLE_NAME.strip():
            raise ValueError("REGISTRY_TABLE_NAME must be configured")

        if not self.REGISTRY_ID.strip():
            raise ValueError("REGISTRY_ID must be configured")

        if not self.PROCESS_AUDIT_TABLE_NAME.strip():
            raise ValueError("PROCESS_AUDIT_TABLE_NAME must be configured")

        if not self.PROCESSING_JOB_JSON.strip() and not self.PROCESSING_JOB_S3_URI:
            raise ValueError(
                "PROCESSING_JOB_JSON or PROCESSING_JOB_S3_URI must be configured"
            )
