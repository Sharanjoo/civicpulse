# CivicPulse — MLK Library Room Analytics

A fault-tolerant ETL pipeline and analytics warehouse for the San José State
University MLK Library's study room booking system. It scrapes booking and
hours data, joins it against the academic calendar, and loads a star schema
that powers the Tableau dashboards below.

**Stack:** Python, SQL, Apache Airflow, PostgreSQL, Docker

## What it does

- Scrapes 100K+ historical and live study room bookings from the SJSU
  library's booking system, plus weekly library hours.
- Cleans, deduplicates, and versions the raw data into a reusable dataset.
- Joins bookings against a hand-built SJSU academic calendar (semesters,
  holidays, exam periods, recess) so demand can be sliced by academic context.
- Loads everything into a Postgres star schema (`dim_date`, `dim_time`,
  `dim_room`, `fact_bookings`) orchestrated end-to-end by Airflow.

![Architecture](Images/Architecture%20Diagram.jpeg)
![Star Schema](Images/Star%20Schema.jpeg)

## Reliability

- **Checkpointed extraction** — `code_files/Bookings.py` and `Hours.py`
  record every completed date/week to a checkpoint file as they scrape.
  A crash or manual stop mid-run leaves the checkpoint intact, so re-running
  the script resumes from where it left off instead of re-scraping months of
  history. A small "refresh window" around today is always re-scraped even
  if already marked complete, since recent bookings change.
- **Fault-tolerant Airflow DAG** — `airflow_project/dags/civicpulse_pipeline_dag.py`
  retries each task twice with a 5-minute backoff, bounds every task to a
  2-hour execution timeout, and wraps every Postgres write in a
  commit/rollback/close context manager so a failed load can't leave a
  connection open or a half-committed table behind.
- **Fail loud, not silent** — if a scrape stops early on an error, the
  extraction script now exits non-zero instead of printing a warning and
  returning success, so Airflow actually retries it rather than silently
  shipping partial data.
- **Idempotent loads** — every load step truncates/`ON CONFLICT DO NOTHING`s
  into place, so re-running the DAG for the same day never double-counts.

## Pipeline

```
create_tables → extract_bookings → extract_hours → transform_data
                                                          │
                             ┌────────────────────────────┼──────────────────────────┐
                             ▼                             ▼                          ▼
                       load_dim_date → load_dim_time → load_dim_room → load_fact_bookings → validate_data
```

| Stage | Script | Purpose |
|---|---|---|
| Extract bookings | `code_files/Bookings.py` | Checkpointed scrape of the room booking API into `csv_txt_files/sjlibrary_bookings.csv` |
| Extract hours | `code_files/Hours.py` | Checkpointed scrape of weekly library hours into `csv_txt_files/sjlibrary_hours.csv` |
| Extract calendar (one-time) | `code_files/Calendars.py` | Parses the SJSU academic calendar PDFs (`data_sources/`) into raw text rows |
| Transform | `code_files/Transform.py` | Builds the star schema CSVs in `warehouse_output/` |
| Load + validate | `airflow_project/dags/civicpulse_pipeline_dag.py` | Loads the warehouse CSVs into Postgres and checks for orphaned foreign keys |

## Running it locally

```bash
cd airflow_project
docker compose up airflow-init
docker compose up -d
```

The DAG (`civicpulse_mlk_library_pipeline`) mounts the repo root itself into
each container at `/opt/airflow/project` (see the `PROJECT_ROOT` volume in
`docker-compose.yaml`), so no per-machine path configuration is needed —
clone the repo anywhere and it works. Open the Airflow UI at
`localhost:8080` and trigger the DAG, or let the daily schedule run it.

## Analysis

Pre-written SQL in `queries/` answers the core demand questions (peak hours,
booking trends, least-utilized rooms, demand by academic context), and
`tableau_dashboard/DWProject.twb` visualizes the resulting warehouse —
snapshots in `Images/`.
