# Cross-Month Order-Independence Test

This fixture verifies that processing `202601` and `202602` in either order
produces the same final `caged_geo_job_metrics` table.

The test uses CBO `622020` (family `6220`) in:

- São Paulo/SP (`355030`)
- Florianópolis/SC (`420540`)
- São José da Tapera/AL (`270840`)

Each month contains current admissions and dismissals. Its exclusion file
contains corrections for the opposite month.

## Expected Final City Metrics

The same values apply to `PROF#6220` and `PROF#ALL`. Each state contains only
one test city, so the state values are equal to the corresponding city values.

| Month | City | Admissions | Dismissals | Salary sum | Salary count |
| --- | --- | ---: | ---: | ---: | ---: |
| `202601` | São Paulo | 1 | 1 | 2000.00 | 1 |
| `202601` | Florianópolis | 2 | 0 | 3000.00 | 2 |
| `202601` | São José da Tapera | 1 | 0 | 2000.00 | 1 |
| `202602` | São Paulo | 1 | 1 | 3000.00 | 1 |
| `202602` | Florianópolis | 2 | 0 | 4500.00 | 2 |
| `202602` | São José da Tapera | 1 | 0 | 3000.00 | 1 |

Expected table counts after both months:

- `caged_geo_job_metrics`: 24
- `caged_metric_batches`: 24
- `caged_metric_revisions`: 24

## Reset Metric Tables

These commands delete all existing metric, batch, and revision data.

```bash
aws dynamodb delete-table \
  --table-name caged_geo_job_metrics \
  --endpoint-url http://127.0.0.1:8000 \
  --region us-east-1

aws dynamodb delete-table \
  --table-name caged_metric_batches \
  --endpoint-url http://127.0.0.1:8000 \
  --region us-east-1

aws dynamodb delete-table \
  --table-name caged_metric_revisions \
  --endpoint-url http://127.0.0.1:8000 \
  --region us-east-1
```

```bash
aws dynamodb create-table \
  --table-name caged_geo_job_metrics \
  --attribute-definitions \
    AttributeName=PK,AttributeType=S \
    AttributeName=SK,AttributeType=S \
    AttributeName=GSI1_PK,AttributeType=S \
    AttributeName=GSI1_SK,AttributeType=S \
  --key-schema \
    AttributeName=PK,KeyType=HASH \
    AttributeName=SK,KeyType=RANGE \
  --global-secondary-indexes \
    '[{"IndexName":"GSI1","KeySchema":[{"AttributeName":"GSI1_PK","KeyType":"HASH"},{"AttributeName":"GSI1_SK","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]' \
  --billing-mode PAY_PER_REQUEST \
  --endpoint-url http://127.0.0.1:8000 \
  --region us-east-1

aws dynamodb create-table \
  --table-name caged_metric_batches \
  --attribute-definitions \
    AttributeName=PK,AttributeType=S \
    AttributeName=SK,AttributeType=S \
  --key-schema \
    AttributeName=PK,KeyType=HASH \
    AttributeName=SK,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --endpoint-url http://127.0.0.1:8000 \
  --region us-east-1

aws dynamodb create-table \
  --table-name caged_metric_revisions \
  --attribute-definitions \
    AttributeName=PK,AttributeType=S \
    AttributeName=SK,AttributeType=S \
  --key-schema \
    AttributeName=PK,KeyType=HASH \
    AttributeName=SK,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --endpoint-url http://127.0.0.1:8000 \
  --region us-east-1
```

## Load Registry Entries

Run from the repository root:

```bash
aws dynamodb update-item \
  --table-name downloaded_files_registry \
  --key '{"registry_id":{"S":"ftp_tree"}}' \
  --update-expression 'SET #tree.#year.#month = :month' \
  --expression-attribute-names \
    '{"#tree":"tree","#year":"2026","#month":"202601"}' \
  --expression-attribute-values \
    file://sample/order_independence/registry_values_202601.json \
  --endpoint-url http://127.0.0.1:8000 \
  --region us-east-1

aws dynamodb update-item \
  --table-name downloaded_files_registry \
  --key '{"registry_id":{"S":"ftp_tree"}}' \
  --update-expression 'SET #tree.#year.#month = :month' \
  --expression-attribute-names \
    '{"#tree":"tree","#year":"2026","#month":"202602"}' \
  --expression-attribute-values \
    file://sample/order_independence/registry_values_202602.json \
  --endpoint-url http://127.0.0.1:8000 \
  --region us-east-1
```

## Run One Month

The shell-provided `PROCESSING_JOB_JSON` overrides the value loaded from
`.env`. `LOCAL_CAGED_SAMPLE_FILES=true` makes `debug_ecs_task.py` copy the TXT
fixtures instead of downloading from S3.

```bash
PROCESSING_JOB_JSON="$(jq -c . \
  sample/order_independence/processing_job_202601.json)" \
python debug_ecs_task.py
```

```bash
PROCESSING_JOB_JSON="$(jq -c . \
  sample/order_independence/processing_job_202602.json)" \
python debug_ecs_task.py
```

## Scenario A

1. Reset the three metric tables.
2. Load both registry entries.
3. Run `202601`.
4. Run `202602`.
5. Save the snapshot:

```bash
aws dynamodb scan \
  --table-name caged_geo_job_metrics \
  --consistent-read \
  --endpoint-url http://127.0.0.1:8000 \
  --region us-east-1 \
  --query 'Items' \
  --output json |
jq -cS '.[]' |
LC_ALL=C sort \
  > snapshots/snapshot_202601_then_202602.jsonl
```

## Scenario B

1. Reset the three metric tables.
2. Load both registry entries again.
3. Run `202602`.
4. Run `202601`.
5. Save the snapshot:

```bash
aws dynamodb scan \
  --table-name caged_geo_job_metrics \
  --consistent-read \
  --endpoint-url http://127.0.0.1:8000 \
  --region us-east-1 \
  --query 'Items' \
  --output json |
jq -cS '.[]' |
LC_ALL=C sort \
  > snapshots/snapshot_202602_then_202601.jsonl
```

## Compare

```bash
wc -l snapshots/snapshot_*.jsonl

sha256sum \
  snapshots/snapshot_202601_then_202602.jsonl \
  snapshots/snapshot_202602_then_202601.jsonl

cmp -s \
  snapshots/snapshot_202601_then_202602.jsonl \
  snapshots/snapshot_202602_then_202601.jsonl

echo $?
```

Exit code `0` means both execution orders produced identical final metrics.
