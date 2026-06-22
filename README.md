# CAGED Processing Task

Dockerized ECS Fargate task that receives a batch of downloaded Novo CAGED
files, groups them by `reference_month`, verifies the required trio
`CAGEDMOV/CAGEDFOR/CAGEDEXC`, updates the downloaded-file registry, and writes
one audit record per file into `caged_processes`.

Complete months are parsed and aggregated into location/profession metrics,
incomplete months are marked as `error`, and the task records one audit item per
file.

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
GEO_JOB_METRICS_TABLE_NAME=caged_geo_job_metrics
CBO_LOOKUP_TABLE_NAME=caged_cbo_lookup
GEO_LOOKUP_TABLE_NAME=caged_geo_lookup
PROCESSING_JOB_JSON=
PROCESSING_JOB_S3_URI=
AWS_REGION=us-east-1
LOG_LEVEL=INFO
```

## Current Processing Behavior

- Group files by `reference_month`.
- Require `CAGEDMOV`, `CAGEDFOR`, and `CAGEDEXC` for a month to be complete.
- Generate one `process_id` UUID per file.
- Parse complete monthly `CAGEDMOV`, `CAGEDFOR`, and `CAGEDEXC` groups.
- Write city/state metrics by CBO family into `caged_geo_job_metrics`.
- Write `PROF#ALL` total metrics for each city/state/month.
- Update the existing `downloaded_files_registry` entry with:
  - `processing_status`
  - `process_id`
  - `updated_at`
- Write one `caged_processes` item per file keyed by:
  - `reference_month`
  - `process_id`
- Continue processing complete months even if another month is incomplete.

## Expected Output From This Process

The processor writes aggregate items to `caged_geo_job_metrics`. Each item is
keyed by one location, one reference month, and one CBO family profession code.

The four main queryable result shapes are:

| Result | PK example | SK example |
| --- | --- | --- |
| State total for a month | `LOC#STATE#35#MONTH#202604` | `PROF#ALL` |
| City total for a month | `LOC#CITY#355030#MONTH#202604` | `PROF#ALL` |
| State + profession for a month | `LOC#STATE#35#MONTH#202604` | `PROF#2237` |
| City + profession for a month | `LOC#CITY#355030#MONTH#202604` | `PROF#2237` |

For example, `PK=LOC#CITY#355030#MONTH#202604` and `SK=PROF#2237`
represents one city's metrics for one profession family in one month. The item
contains numeric fields such as `admissions`, `dismissals`, `net_balance`,
`total_turnover`, `salary_sum`, `salary_count`, and `avg_salary`, plus
denormalized labels like `location_name`, `state_name`, and `family_title`.

Querying only the `PK` returns all profession-family rows for that
city/state/month, including the `PROF#ALL` total row.

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
