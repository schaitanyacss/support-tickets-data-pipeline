# 🏥 CarePlus - Serverless AWS Data Pipeline

> An end-to-end, event-driven ELT pipeline that turns messy support tickets and backend logs into a live Power BI dashboard.

[![Status](https://img.shields.io/badge/status-complete-brightgreen)]()
[![Type](https://img.shields.io/badge/type-data%20pipeline-blue)]()
[![Industry](https://img.shields.io/badge/industry-insurance-informational)]()
[![Tool](https://img.shields.io/badge/tool-Python%20&#124;%20AWS%20&#124;%20Power%20BI-orange)]()

---

## 📌 TL;DR

CarePlus is a fictional healthcare-support company that generates two very different flavors of raw data every day: **unstructured application logs** (`.log`) and **structured support tickets** (MySQL → `.csv`). This project builds a **fully serverless, event-triggered pipeline on AWS** that ingests both, cleans and standardizes them independently, lands them as **Parquet** in a data lake, makes them queryable via **Athena** for ad-hoc analysis, and loads them into **Amazon Redshift Serverless** for BI consumption through a **Power BI** dashboard.

**Highlights:**
- 🔁 Two independent ingestion → transformation pipelines (logs vs. tickets), unified at the warehouse layer
- ⚡ Fully automated with **S3 event triggers → Lambda / Glue**, no manual re-runs needed after the initial backfill
- 🧹 Real data-quality engineering: regex log parsing, typo standardization, sentinel-value handling, dedup, type casting
- 📊 Two consumption paths: **Athena** for ad-hoc SQL and **Redshift + Power BI** for a persistent dashboard
- 📦 ~2,650 backend log events and ~800+ ticket records processed across a 31-day synthetic dataset

---

## 📖 Table of Contents

- [Business Context](#-business-context)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Data Model](#-data-model)
- [Pipeline Walkthrough](#-pipeline-walkthrough)
- [Data Quality: What Was Actually Wrong With the Data](#-data-quality-what-was-actually-wrong-with-the-data)
- [Sample Analytics Queries](#-sample-analytics-queries)
- [Dashboard](#-dashboard)
- [Repository Structure](#-repository-structure)
- [How to Reproduce This Project](#-how-to-reproduce-this-project)
- [Security Notes](#-security-notes)
- [Future Improvements](#-future-improvements)
- [Acknowledgments](#-acknowledgments)

---

## 🏢 Business Context

CarePlus's customer support org produces two related but disconnected data streams:

| Source | What it captures | Native format | System of record |
|---|---|---|---|
| **Support Tickets** | Customer-reported issues: priority, agent, status, channel, resolution time | Relational rows | MySQL (`careplus_support_db`) |
| **Support Logs** | Backend telemetry for every ticket interaction: response time, CPU load, errors, session/user-agent info | Semi-structured text | Flat `.log` files |

The two datasets share a `ticket_id` in a **one-to-many** relationship (one ticket → many backend log events), but they live in different systems, in different formats, on different schedules. The goal of this project was to build a pipeline that unifies them into a single warehouse so support-ops and engineering can answer questions like *"which channel generates the most tickets?"* or *"is high CPU load correlated with slower response times?"* from one dashboard.

---

## 🏗 Architecture

![Pipeline Architecture](pipeline_diagram.jpg)

**Flow summary:**
1. **Ingestion** — Python scripts pull daily `.log` files and query MySQL for the previous day's tickets, then push both to an S3 **raw** zone.
2. **Transformation (event-driven)** — an S3 `PUT` event on the raw prefix fires a Lambda:
   - **Logs** → AWS **Lambda** parses the log grammar with regex, cleans it, and writes **Parquet** to the **processed** zone.
   - **Tickets** → Lambda triggers an AWS **Glue** job that cleans the CSV and writes **Parquet** to the **processed** zone.
3. **Ad-hoc analysis** — **Amazon Athena** queries the processed Parquet directly for exploratory SQL, no warehouse load required.
4. **Warehousing** — a second S3 event trigger fires a Lambda that runs a Redshift `COPY` command, incrementally loading new Parquet files into **Amazon Redshift Serverless**.
5. **Visualization** — **Power BI** connects to Redshift and refreshes the *CarePlus Insights* dashboard.

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

## 🗃 Data Model

**`support_tickets`**

| Column | Type | Notes |
|---|---|---|
| `ticket_id` | VARCHAR | e.g. `TCK0701011` — may repeat (see Data Quality) |
| `created_at` | TIMESTAMP | When the ticket was logged |
| `resolved_at` | TIMESTAMP | Nullable if not yet resolved |
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
- `support_logs_ingestion_to_S3.ipynb` reads the next unprocessed day's `.log` file and uploads it to `s3://<bucket>/support-logs/raw/`, tracking progress in a local date-tracker file so re-runs pick up exactly where they left off.
- `support_tickets_ingestion_to_S3.ipynb` queries MySQL for the previous day's tickets via `SQLAlchemy`/`pandas`, and uploads the result as CSV to `s3://<bucket>/support-tickets/raw/`.
- Credentials are loaded from a local `.env` file (see `sample.env`) and are **never hardcoded**.

### 2. Transformation (`data-transformation/`)
- **Logs → Lambda:** an S3 `ObjectCreated` event on the raw logs prefix invokes a Lambda that:
  - Parses each entry with a regex tuned to the log grammar (`timestamp`, `log_level`, `component`, `TicketID`, `SessionID`, `IP`, `ResponseTime`, `CPU`, `EventType`, `Error`, `UserAgent`, `Message`, `Debug`, `TraceID`)
  - Drops the unused `trace_id` field
  - Casts `response_time` → int, `cpu` → float, `error` → boolean, `timestamp` → datetime
  - Writes the cleaned DataFrame to S3 as Parquet using `pyarrow`
- **Tickets → Glue:** the same event pattern triggers a Lambda that calls `glue.start_job_run()` on an AWS Glue job, passing the S3 input path as a job argument. The Glue job standardizes and writes the cleaned data to `support-tickets/processed/` as Parquet.

### 3. Ad-hoc Analysis (`data-warehousing-analytics/athena-sql-queries/`)
- An Athena database (`careplus_db`) is created directly over the `processed/` Parquet prefixes, letting analysts run SQL against the data lake with zero infrastructure and zero cost when idle.

### 4. Warehousing (`data-warehousing-analytics/redshift-setup/`)
- Tables are created in Redshift Serverless matching the cleaned schema.
- Initial load uses a `COPY ... FORMAT AS PARQUET` statement.
- Ongoing loads are automated: a second S3 event trigger invokes a Lambda (`psycopg2`) that runs an incremental `COPY` for each newly landed Parquet file, keeping Redshift in sync with the data lake without manual intervention.

### 5. Dashboard (`data-warehousing-analytics/dashboard/`)
- Power BI connects directly to Redshift Serverless and visualizes ticket volume, channel mix, resolution status, and backend health metrics in **`Careplus Insights.pbix`**.

---

## 🧹 Data Quality: What Was Actually Wrong With the Data

This project deliberately works with messy, realistic data rather than a clean tutorial dataset. Issues discovered and handled during transformation:

- **Categorical typos** in `priority` and `log_level` — e.g. `Lw` → `Low`, `Hgh` → `High`, `Medum` → `Medium`, `INF0` → `INFO`, `DEBG` → `DEBUG`, `warnING` → `WARNING`, `EROR` → `ERROR`
- **Sentinel/placeholder values** — `num_interactions` occasionally contains `-999999` in place of a true null, requiring explicit handling rather than naive averaging
- **Invalid measurements** — a small number of log rows have a negative `response_time`, which is physically meaningless and filtered out
- **Duplicate records** — both the raw ticket dump and raw logs contain exact duplicate rows that inflate counts if not deduplicated before load
- **Mixed date formats** — `created_at` / `resolved_at` and log `timestamp` values needed explicit parsing and casting to a consistent `TIMESTAMP` type
- **Unstructured text at scale** — the log format has no schema at all; a hand-built regex reconstructs a structured record from ~2,650 free-text entries

---

## 📊 Sample Analytics Queries

Run from `data-warehousing-analytics/athena-sql-queries/sql-queries.txt` against the processed Parquet in Athena:

```sql
-- Ticket load by channel — understand user preference & staffing needs
SELECT channel, COUNT(*) AS ticket_count
FROM support_tickets_processed
GROUP BY channel
ORDER BY ticket_count DESC;

-- Average CPU usage per user agent — spot backend stress by client type
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

More queries — ticket status breakdown, debug-level event counts, and event volume per user agent — are in the full [`sql-queries.txt`](data-warehousing-analytics/athena-sql-queries/sql-queries.txt).

---

## 📈 Dashboard

The Redshift-backed Power BI dashboard (**`Careplus Insights.pbix`**) surfaces:
- Ticket volume and resolution rate by channel, priority, and agent
- Escalation trends over time
- Backend health signals (response time, CPU load, error rate) tied back to ticket volume

> 💡 *Add a screenshot or exported PDF of your dashboard here — recruiters and reviewers engage far more with a visual than a filename.*

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

---

## ⚙️ How to Reproduce This Project

1. **Set up source data**
   - Restore `careplus_support_db.sql` into a local/managed MySQL instance for the tickets source.
   - Use the sample `.log` files (or generate your own) for the logs source.
2. **Configure AWS resources**
   - Create an S3 bucket with `raw/` and `processed/` prefixes for both `support-logs` and `support-tickets`.
   - Create the Lambda functions (log parser, Glue trigger, Redshift loader) and attach S3 event notifications on the relevant prefixes.
   - Create a Glue job for ticket cleaning and Parquet conversion.
   - Create an Athena database over the processed prefixes.
   - Provision Redshift Serverless and run the DDL in `table-creation-queries.txt`.
3. **Set environment variables** — copy `sample.env` → `.env` in each ingestion folder and fill in your own AWS/MySQL credentials. **Never commit `.env` files.**
4. **Run ingestion notebooks** to backfill historical data, then let the event triggers take over for new files.
5. **Connect Power BI** to your Redshift Serverless endpoint and open `Careplus Insights.pbix`, or rebuild the report against your own workspace.

---

## 🔒 Security Notes

- All AWS/DB credentials are supplied via `.env` files (see `sample.env` templates) and loaded with `python-dotenv` — **no secrets are hardcoded in source**.
- **Before pushing this repo publicly**, double-check every notebook for accidentally hardcoded values (connection strings, passwords, IAM role ARNs, account IDs) left over from local testing, and replace them with placeholders or environment variables. A `.gitignore` excluding `.env`, `*.pbix` data caches, and local credential files is strongly recommended.
- For a production version of this pipeline, credentials should live in **AWS Secrets Manager** or **Systems Manager Parameter Store** rather than `.env` files, and the Redshift loader Lambda should assume an IAM role instead of using a database password directly.

---

## 🚀 Future Improvements

- Replace ad-hoc Lambda triggers with **Step Functions** or **Managed Airflow (MWAA)** for observable, retryable orchestration
- Add automated data-quality checks (e.g. **Great Expectations** or dbt tests) before data lands in Redshift
- Move infrastructure definition into **Terraform/CloudFormation** for repeatable, version-controlled deployments
- Add CI (GitHub Actions) to lint/test the Lambda and Glue scripts on every commit
- Partition Parquet output by date to speed up Athena scans as data volume grows
- Add a dbt layer on top of Redshift for versioned, testable transformation logic

---

## 🙏 Acknowledgments

This project was built as a hands-on, guided data engineering exercise on the AWS stack, then extended with a custom synthetic dataset, original data-cleaning logic, Athena/Redshift SQL, and a Power BI dashboard built from scratch.

---

## 👤 Author

**Your Name**
📧 your.email@example.com · 🔗 [LinkedIn](https://linkedin.com/in/yourprofile) · 💻 [GitHub](https://github.com/yourusername)
