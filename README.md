# CAGED Processing Task

Dockerized ECS Fargate task that receives a batch of downloaded Novo CAGED
files, groups them by `reference_month`, verifies the required trio
`CAGEDMOV/CAGEDFOR/CAGEDEXC`, updates the downloaded-file registry, and writes
one audit record per file into `caged_processes`.

This phase stops before archive parsing. Complete months are marked as
`processing`, incomplete months are marked as `error`, and the task returns a
placeholder `"ok"` response.

## Job Input

Pass the downloaded batch payload through `PROCESSING_JOB_JSON` or
`PROCESSING_JOB_S3_URI`. The payload shape matches
[`sample/received_payload.json`](sample/received_payload.json).

```json
{
  "status": "COMPLETED",
  "files": [
    {
      "status": "downloaded",
      "filename": "CAGEDEXC202604.7z",
      "reference_month": "202604",
      "reference_year": "2026",
      "s3_bucket": "caged-dev-downloaded-files-123456789012",
      "s3_key": "raw/caged/year=2026/month=04/file_type=exclusion/CAGEDEXC202604.7z",
      "s3_uri": "s3://caged-dev-downloaded-files-123456789012/raw/caged/year=2026/month=04/file_type=exclusion/CAGEDEXC202604.7z",
      "size_bytes": 140967
    }
  ]
}
```

`PROCESSING_JOB_JSON` takes precedence when both variables are set.

## Environment Variables

```env
REGISTRY_TABLE_NAME=downloaded_files_registry
REGISTRY_ID=ftp_tree
PROCESS_AUDIT_TABLE_NAME=caged_processes
PROCESSING_JOB_JSON=
PROCESSING_JOB_S3_URI=
AWS_REGION=us-east-1
LOG_LEVEL=INFO
```

## Current Processing Behavior

- Group files by `reference_month`.
- Require `CAGEDMOV`, `CAGEDFOR`, and `CAGEDEXC` for a month to be complete.
- Generate one `process_id` UUID per file.
- Update the existing `downloaded_files_registry` entry with:
  - `processing_status`
  - `process_id`
  - `updated_at`
- Write one `caged_processes` item per file keyed by:
  - `reference_month`
  - `process_id`
- Continue processing complete months even if another month is incomplete.

## Development

```bash
uv sync --all-groups
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

## Deployment

The `Deploy Processing Task` GitHub Actions workflow builds the Docker image,
tags it with the commit SHA, pushes it to ECR, renders the current ECS task
definition family with that image, and registers a new task-definition revision.
It does not run the ECS task during deployment.

Step Functions runs the latest active `caged-dev-processing-task` task
definition revision when the download workflow produces a completed payload.

## Local Debug

Use `debug_handler.py` to run the task locally through the same `app.cli.main()`
entrypoint used by the container:

```bash
uv run python debug_handler.py
```

Default behavior:

- loads `sample/received_payload.json` into `PROCESSING_JOB_JSON`
- points DynamoDB to `http://127.0.0.1:8000`
- uses local placeholder AWS credentials unless you override them

Override any env var before running when you want a different payload, region,
or DynamoDB endpoint.
