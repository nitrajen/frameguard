# dfguard Airflow example

A two-task Airflow DAG (`orders_pipeline`) that reads raw orders from Parquet,
enriches them, and writes a customer summary. dfguard enforces schema at every
stage boundary.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Run

```bash
export AIRFLOW_HOME=$(pwd)
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags

airflow db migrate
airflow dags backfill --start-date 2024-01-01 --end-date 2024-01-01 orders_pipeline
```

The first task reads from `/tmp/orders_data/raw_orders.parquet` and writes enriched
output; the second reads that output and writes a summary. If either parquet file has
an unexpected schema, dfguard raises before any processing starts.
