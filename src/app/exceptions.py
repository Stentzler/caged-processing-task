class ProcessingTaskError(Exception):
    """Base exception for processing task errors."""


class InvalidProcessingJobError(ProcessingTaskError):
    """Raised when the processing job payload is invalid."""


class ProcessingFailedError(ProcessingTaskError):
    """Raised when CAGED processing fails after status tracking starts."""
