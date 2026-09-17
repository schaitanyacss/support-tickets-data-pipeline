# 🏥 CarePlus - Serverless AWS Data Pipeline

> An end-to-end, event-driven ELT pipeline that turns messy support tickets and backend logs into a live Power BI dashboard.

[![Status](https://img.shields.io/badge/status-complete-brightgreen)]()
[![Type](https://img.shields.io/badge/type-data%20pipeline-blue)]()
[![Industry](https://img.shields.io/badge/industry-support-informational)]()
[![Tool](https://img.shields.io/badge/tool-MySQL%20&#124;%20Python%20&#124;%20AWS%20&#124;%20Power%20BI-orange)]()

---

## 📖 Background & Overview

CarePlus is a fictional healthcare-support company that generates two very different flavors of raw data every day: **unstructured application logs** (`.log`) and **structured support tickets** (MySQL → `.csv`). This project builds a **fully serverless, event-triggered pipeline on AWS** that ingests both, cleans and standardizes them independently, lands them as **Parquet** in a data lake, makes them queryable via **Athena** for ad-hoc analysis, and loads them into **Amazon Redshift Serverless** for BI consumption through a **Power BI** dashboard.

**Highlights:**
- Two independent ingestion → transformation pipelines (logs vs. tickets), unified at the warehouse layer
- Fully automated with **S3 event triggers → Lambda / Glue**, no manual re-runs needed after the initial backfill
- Real data-quality engineering: regex log parsing, typo standardization, sentinel-value handling, dedup, type casting
- Two consumption paths: **Athena** for ad-hoc SQL and **Redshift + Power BI** for a persistent dashboard

---

## 🏢 Business Context

CarePlus's customer support org produces two related but disconnected data streams:

| Source | What it captures | Native format | System of record |
|---|---|---|---|
| **Support Tickets** | Customer-reported issues: priority, agent, status, channel, resolution time | Relational rows | MySQL (`careplus_support_db`) |
| **Support Logs** | Backend telemetry for every ticket interaction: response time, CPU load, errors, session/user-agent info | Semi-structured text | Flat `.log` files |

The two datasets share a `ticket_id` in a **one-to-many** relationship (one ticket → many backend log events), but they live in different systems, in different formats, on different schedules. The goal of this project was to build a pipeline that unifies them into a single warehouse so support-ops and engineering can access and draw insights from the data.

---

## 🏗 Architecture

<img width="703" height="434" alt="image" src="https://github.com/user-attachments/assets/7482db6f-7873-451c-8e4a-3ab0ee2d4f50" />

**Flow summary:**
1. **Ingestion** - Python scripts pull daily `.log` files and query MySQL for the previous day's tickets, then push both to an S3 **raw** zone.
2. **Transformation (event-driven)** - an S3 `PUT` event on the raw prefix fires a Lambda:
   - **Logs** → AWS **Lambda** parses the log grammar with regex, cleans it, and writes **Parquet** to the **processed** zone.
   - **Tickets** → Lambda triggers an AWS **Glue** job that cleans the CSV and writes **Parquet** to the **processed** zone.
3. **Ad-hoc analysis** - **Amazon Athena** queries the processed Parquet directly for exploratory SQL, no warehouse load required.
4. **Warehousing** - a second S3 event trigger fires a Lambda that runs a Redshift `COPY` command, incrementally loading new Parquet files into **Amazon Redshift Serverless**.
5. **Visualization** - **Power BI** connects to Redshift and refreshes the *CarePlus* dashboard.

---

## 🗃 Data Model

- Fact tables - **`support_tickets`** , **`support_logs`**
- Dimension tables - **`dim_date`** , **`dim_tickets`**
- Measures table - **`key_measures`**
  
<img width="545" height="406" alt="image" src="https://github.com/user-attachments/assets/f30b19fd-20e5-44a5-878e-184a804264eb" />

**`support_tickets`**

| Column | Type | Notes |
|---|---|---|
| `ticket_id` | VARCHAR | e.g. `TCK0701011` - may repeat |
| `created_at` | TIMESTAMP | When the ticket was logged |
| `resolved_at` | TIMESTAMP | Null if not yet resolved |
| `agent` | VARCHAR | Assigned support agent |
| `priority` | VARCHAR | Low / Medium / High |
| `issue_category` | VARCHAR | e.g. Bug Report, Login Issue, Payment Failure |
| `num_interactions` | BIGINT | Number of back-and-forth touches on the ticket |
| `status` | VARCHAR | Resolved / Open / Escalated |
| `channel` | VARCHAR | Email / Chat / Phone / Web Form |

**`support_logs`**

| Column | Type | Notes |
|---|---|---|
| `timestamp` | TIMESTAMP | Backend event time |
| `log_level` | VARCHAR | INFO / DEBUG / WARNING / ERROR |
| `component` | VARCHAR | Backend service that emitted the event |
| `ticket_id` | VARCHAR | Foreign key to `support_tickets` |
| `session_id` | VARCHAR | Session tied to the event |
| `ip` | VARCHAR | Client IP |
| `response_time` | BIGINT | Milliseconds |
| `cpu` | DOUBLE PRECISION | CPU load at event time (%) |
| `event_type` | VARCHAR | Event classification |
| `error` | BOOLEAN | Whether the event recorded an error |
| `user_agent` | VARCHAR | Client / tool used |
| `message` | VARCHAR | Free-text event message |
| `debug` | VARCHAR | Internal debug note |

**Relationship:** `support_tickets.ticket_id (1) → support_logs.ticket_id (N)`

---

## 🔄 Pipeline Walkthrough

### 1. Ingestion (`data-ingestion/`)
- `S3_support_logs_ingestion.py` reads the next unprocessed day's `.log` file and uploads it to `s3://<bucket>/support-logs/raw/`, tracking progress in a local date-tracker file so re-runs pick up exactly where they left off.
- `S3_support_tickets_ingestion.py` queries MySQL for the previous day's tickets via `SQLAlchemy`/`pandas`, and uploads the result as CSV to `s3://<bucket>/support-tickets/raw/`.
- Credentials are loaded from a local `.env` file (see `sample.env`) and are **never hardcoded**.

### 2. Transformation (`data-transformation/`)
- **Logs → Lambda:** an S3 `ObjectCreated` event on the raw logs prefix invokes a Lambda that:
  - Parses each entry with a regex tuned to the log grammar (`timestamp`, `log_level`, `component`, `TicketID`, `SessionID`, `IP`, `ResponseTime`, `CPU`, `EventType`, `Error`, `UserAgent`, `Message`, `Debug`, `TraceID`)
  - Drops the unused `trace_id` field
  - Casts `response_time` → int, `cpu` → float, `error` → boolean, `timestamp` → datetime
  - Writes the cleaned DataFrame to S3 as Parquet using `pyarrow`
- **Tickets → Glue:** the same event pattern triggers a Lambda that calls `glue.start_job_run()` on an AWS Glue job, passing the S3 input path as a job argument. The Glue job standardizes and writes the cleaned data to `s3://<bucket>/support-tickets/processed/` as Parquet.

### 3. Ad-hoc Analysis (`data-warehousing/athena/`)
- An Athena database (`careplus_db`) is created directly over the `processed/` Parquet prefixes, letting analysts run SQL against the data lake with zero infrastructure and zero cost when idle.

### 4. Warehousing (`data-warehousing/redshift/`)
- Tables are created in Redshift Serverless matching the cleaned schema.
- Initial load uses a `COPY ... FORMAT AS PARQUET` statement.
- Ongoing loads are automated: a second S3 event trigger invokes a Lambda (`psycopg2`) that runs an incremental `COPY` for each newly landed Parquet file, keeping Redshift in sync with the data lake without manual intervention.

### 5. Dashboard (`dashboard/`)
- Power BI connects directly to Redshift Serverless and visualizes ticket volume, channel mix, resolution status, and backend health metrics in **`CarePlus.pbix`**.

---

## 🧹 Data Quality Checks

Issues discovered and handled during transformation:

- **Categorical typos** in `priority` and `log_level` - e.g. `Lw` → `Low`, `Hgh` → `High`, `Medum` → `Medium`, `INF0` → `INFO`, `DEBG` → `DEBUG`, `warnING` → `WARNING`, `EROR` → `ERROR`
- **Sentinel/placeholder values** - `num_interactions` occasionally contains `-999999` in place of a true null, requiring explicit handling rather than naive averaging
- **Invalid measurements** - a small number of log rows have a negative `response_time`, which is physically meaningless and filtered out
- **Duplicate records** - both the raw ticket dump and raw logs contain exact duplicate rows that inflate counts if not deduplicated before load
- **Mixed date formats** - `created_at` / `resolved_at` and log `timestamp` values needed explicit parsing and casting to a consistent `TIMESTAMP` type
- **Unstructured text at scale** - the log format has no schema at all; a regex logic reconstructs structured records
---

## 📊 Sample Analytics Queries

Run from `data-warehousing-analytics/athena-sql-queries/sql-queries.txt` against the processed Parquet in Athena:

```sql
-- Ticket load by channel - understand user preference & staffing needs
SELECT channel, COUNT(*) AS ticket_count
FROM support_tickets_processed
GROUP BY channel
ORDER BY ticket_count DESC;

-- Average CPU usage per user agent - spot backend stress by client type
SELECT user_agent, AVG(cpu) AS avg_cpu_usage
FROM support_logs_processed
GROUP BY user_agent
ORDER BY avg_cpu_usage DESC;

-- Daily ticket trend
SELECT DATE(created_at) AS day, COUNT(*) AS tickets_created
FROM support_tickets_processed
GROUP BY DATE(created_at)
ORDER BY day;
```

More queries - ticket status breakdown, debug-level event counts, and event volume per user agent, are in the full [`sql-queries.txt`](data-warehousing-analytics/athena-sql-queries/sql-queries.txt).

---

## 📈 Dashboard

The Redshift-backed Power BI dashboard (**`CarePlus.pbix`**) surfaces:
- Ticket volume by agent, channel and status
- Ticket resolution rate by issue category and priority
- Backend health signals (logged tickets, avg response time, log level) tied back to ticket volume
- CPU load trend over time

<img width="844" height="406" alt="image" src="https://github.com/user-attachments/assets/318e5596-23da-4cc5-9243-379b697df397" />

<img width="847" height="404" alt="image" src="https://github.com/user-attachments/assets/a55ce8ec-3ec9-4ab2-a7c7-91379192fac5" />

---

## 🛠 Tech Stack

| Layer | Service / Tool | Purpose |
|---|---|---|
| Ingestion | Python, `boto3`, `pandas`, `SQLAlchemy` | Pull data from MySQL / local `.log` files into S3 |
| Storage (data lake) | Amazon S3 | `raw/` and `processed/` zones per data source |
| Compute (logs) | AWS Lambda + `pyarrow` | Regex parsing, cleaning, Parquet conversion |
| Compute (tickets) | AWS Glue | Batch ETL and Parquet conversion for tabular data |
| Orchestration | S3 Event Notifications | Trigger Lambda/Glue automatically on new file arrival |
| Ad-hoc analytics | Amazon Athena | Serverless SQL directly over S3 Parquet |
| Warehouse | Amazon Redshift Serverless | `COPY FROM ... FORMAT AS PARQUET` incremental loads |
| BI / Visualization | Power BI | Live dashboard connected to Redshift |
| Config | `python-dotenv`, `.env` | Local credential management (never committed) |

---

## 📁 Repository Structure

```
project-care-plus/
├── data-ingestion/
│   ├── support-logs/
│   │   ├── support_logs_ingestion_to_S3.ipynb
│   │   ├── log_date_tracker.txt
│   │   └── sample.env
│   └── support-tickets/
│       ├── support_tickets_ingestion_to_S3.ipynb
│       ├── date_tracker.txt
│       └── sample.env
├── data-transformation/
│   ├── support-log-transformation/
│   │   └── automate_support_log_ETL.ipynb      # Lambda: regex-parse & clean logs → Parquet
│   └── support-tickets-transformation/
│       └── automate_support_tickets_ETL.ipynb  # Lambda: triggers Glue job
├── data-warehousing-analytics/
│   ├── athena-sql-queries/
│   │   └── sql-queries.txt
│   ├── redshift-setup/
│   │   ├── table-creation-queries.txt
│   │   └── incremental-data-loading-logs.ipynb # Lambda: incremental COPY into Redshift
│   └── dashboard/
│       └── Careplus Insights.pbix
├── meta_data.txt          # Data dictionary for both tables
├── pipeline_diagram.jpg   # Architecture diagram (referenced above)
└── README.md
```
