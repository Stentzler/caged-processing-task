# SPEC.md: CAGED Data Ingestion & Analytics Pipeline

## 1. System Architecture Overview

The objective of this pipeline is to ingest massive monthly CSV microdata files from the official Brazilian Ministry of Labor (MTE) FTP server (`MOV`, `FOR`, `EXC`), clean and aggregate them in-memory, and upsert the summarized metrics into Amazon DynamoDB.

This architecture leverages Pre-Aggregated Metrics to avoid storing raw rows, shrinking storage requirements by over 90% while allowing the frontend API to query state, city, demographic, and job leadership metrics in sub-millisecond speeds.

---

## 2. Raw Input Data Samples

The pipeline consumes three distinct semi-colon (`;`) separated files every month. The files use `ISO-8859-1` (Latin-1) encoding and format numbers with commas for decimals.

### 2.1 Current Month Movements (`CAGEDMOV`)

Contains formal job records that occurred and were reported on time within the reference month.
Sample at [text](sample/CAGEDMOV.csv)

### 2.2 Late Submissions (`CAGEDFOR`)

Contains historical formal job records that occurred in previous months but were only declared by companies during the current processing month.
Sample at [text](sample/CAGEDFOR.csv)

### 2.3 Historical Exclusions (`CAGEDEXC`)

Contains error-correction records indicating entries from *previous* months that the government is retroactively deleting.
Sample at [text](sample/CAGEDEXC.csv)

---

## 3. Reference Data Schemas (The Dictionary Layer)

These static lookup reference tables resolve the numerical and structural tokens found inside raw CAGED files into descriptive human-readable text strings during API retrieval.

### 3.1 Occupations: `caged_cbo_lookup`
* **Partition Key (PK):** `code` (String, e.g., "317110")
* **Attributes:** `job_title`, `family_code`, `family_title`

### 3.2 Geography: `caged_geo_lookup`
* **Partition Key (PK):** `code` (String, e.g., "35" or "355030")
* **Attributes:** `type` (CITY/STATE), `name`, `state_code`, `state_name`, `region_name`

### 3.3 Corporate Business Activities: `caged_cnae_lookup`
* **Partition Key (PK):** `code` (String, e.g., "J" or "6201501")
* **Attributes:** `type` (SECTION/SUBCLASS), `description`, `parent_section_code`, `parent_section_name`

### 3.1 Data Sample

```json
[
  {
    "code": "317110",
    "title": "Sistemas de Informação - Desenvolvedor",
    "family_code": "3171",
    "family_title": "Profissionais em Desenvolvimento de Sistemas (TI)"
  },
  {
    "code": "223115",
    "title": "Médico Clínico Geral",
    "family_code": "2231",
    "family_title": "Médicos Clínicos"
  }
]

```

---

## 4. System Interactions & Data Flow

To ensure high performance and low storage costs, the transactional records and the reference labels interact using a **Split-Layer Lookup Pattern**.

```
   INGESTION PHASE                          API QUERY PHASE
┌──────────────────┐                     ┌────────────────────┐
│   MOV/FOR/EXC    │                     │   Frontend Client  │
└────────┬─────────┘                     └─────────┬──────────┘
         │                                         │
         │ Aggregates by                           │ 1. Get Metrics
         │ Location + Code                         ▼
         ▼                               ┌────────────────────┐
┌──────────────────┐                     │    API Gateway     │
│   DynamoDB       │                     └────┬──────────┬────┘
│  Analytics Tabs  │                          │          │
└──────────────────┘            2. Fetch Core │          │ 3. Fetch Labels
                                   Metrics    ▼          ▼
                                 ┌──────────────┐      ┌────────────────────┐
                                 │ caged_geo_...│      │caged_cbo_lookup    │
                                 └──────────────┘      └────────────────────┘

```

### 4.1 Ingestion Phase: The Analytical Delta Calculation

When processing a batch of monthly files, the pipeline aggregates records on a mathematical matrix before touching DynamoDB:

1. **Parse `MOV` & `FOR`:** Clean numeric elements (e.g., convert `1654,62` to float `1654.62`). Accumulate admissions and dismissals.
2. **Parse `EXC`:** Look at the **`competênciamov`** attribute. Find that historical index in your in-memory map. **Invert the operation** to undo the historic mistake:
* If `saldomovimentação == 1` in `EXC`, subtract $1$ from `admissions`.
* If `saldomovimentação == -1` in `EXC`, subtract $1$ from `dismissals`.



### 4.2 Query Phase: Resolving Text Labels via API

To keep the main transactional tables as lean as possible, **do not write text strings into the transaction tables**. Instead, allow the API to join them dynamically:

1. The user requests a dashboard for a specific city.
2. The API queries the transactional table to fetch the top 10 performing CBO codes (e.g., `317110`).
3. The API performs a quick batch read (`BatchGetItem`) against `caged_cbo_lookup` to pull the matching `"job_title"` and `"family_title"`.
4. The API builds the clean response payload for the frontend UI.

---

## 5. Main Analytics Database Schema

### 5.1 Table A: Geo & Job Analytics (`caged_geo_job_metrics`)

* **Partition Key (PK):** `LOC#<Location_ID>#JOB#<CBO_Code>`
* *City Example:* `LOC#CITY#355030#JOB#317110`
* *State Example:* `LOC#STATE#35#JOB#317110`


* **Sort Key (SK):** `MONTH#<YYYYMM>` (e.g., `MONTH#202604`)

#### Core Attributes & Global Secondary Index (GSI)

To allow dynamic sorting (leaderboards) by net job gains or highest entry wages, a Global Secondary Index (`GSI1`) is implemented.

| Field Name | Type | Description |
| --- | --- | --- |
| `admissions` | Number | Count of hires where `saldomovimentação == 1` |
| `dismissals` | Number | Count of fires where `saldomovimentação == -1` |
| `net_balance` | Number | Real employment growth ($admissions - dismissals$) |
| `total_turnover` | Number | Job volatility/shifting ($admissions + dismissals$) |
| `avg_salary` | Number | Calculated average of `salário` for the admissions |
| **`GSI1_PK`** | String | **GSI Partition Key:** `LOC#<Location_ID>#MONTH#<YYYYMM>` |
| **`GSI1_SK`** | String | **GSI Sort Key:** Maps to metric being ranked (e.g., `net_balance`) |

### 5.2 Table B: Demographic Metrics (`caged_demographic_metrics`)

Isolates socio-demographic features to prevent combination explosion in Table A.

* **Partition Key (PK):** `LOC#<Location_ID>#SEX#<sex_id>` (1 = Male, 3 = Female)
* **Sort Key (SK):** `MONTH#<YYYYMM>`

---

## 6. Core Business Metrics Equations

The API uses the stored table properties to compute the following high-value market insights:

1. **Net Job Balance (Saldo Líquido):** 
$$admissions - dismissals$$



*Indicates whether an industry or region's labor force is structurally expanding or shrinking.*
2. **Turnover Rate / Job Shifting (Rotatividade):** 
$$\frac{admissions + dismissals}{2}$$



*Identifies fields with massive movement but low net growth (high churn environments, which represent high recruitment costs for businesses).*
3. **The Local Wage Premium:** 
$$\text{City } avg\_salary - \text{State } avg\_salary$$



*Helps workers analyze whether relocating to a specific city increases their earnings potential relative to the state baseline for their job code.*

---

Now that your reference table is set up and our specifications are completely updated to reflect your new setup, would you like to start building the core aggregation loop script that handles the `MOV`, `FOR`, and `EXC` delta matching logic?