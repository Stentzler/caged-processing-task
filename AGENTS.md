# Project Architecture

## Overview

This repository contains one Dockerized ECS Fargate task that processes one
monthly Novo CAGED grouped job. Step Functions must pass CAGEDMOV, CAGEDFOR,
and CAGEDEXC together for the same competence/month.

Follow the global `clean-code` skill for general implementation quality.

## Structure

- `src/app/__main__.py`: module entry point for `python -m app`.
- `src/app/cli.py`: process wiring, settings loading, AWS client creation, and
  exit-code handling. Keep it thin.
- `src/app/models.py`: validated job input and result models.
- `src/app/service.py`: orchestration, processing status transitions, and error
  handling.
- `src/app/processor.py`: CAGED-specific parsing and business rules.
- `src/app/settings.py`: environment-backed runtime configuration.
- `src/app/aws.py`: AWS client/resource helpers.
- `tests/`: unit tests with fakes.

## Implementation Conventions

- Keep ECS entrypoint code thin: `Dockerfile`, `src/app/__main__.py`, and
  `src/app/cli.py` should only start the container process, load settings,
  configure dependencies, call `ProcessingService`, and return the exit code.
- Keep reusable infrastructure helpers in `caged-serverless-toolkit`. This
  includes generic AWS helpers from `src/app/aws.py` and shared logging setup
  from `src/app/logging.py`; this task should keep only app-specific wrappers
  or orchestration code.
- Keep CAGED-specific rules in `processor.py`.
- Inject external dependencies into services so tests do not require AWS access.
- Read configuration through `Settings`; avoid scattered environment lookups.
- Preserve the grouped processing contract: all three files are required.
- Add focused tests for new business rules and output fields.

## Validation

Run before finishing code changes:

```bash
uv run pytest
uv run ruff check .
```
