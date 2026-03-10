from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from utils.db import get_conn


def extract():
    rows = fetch_all("""
        SELECT event_time, user_id, page, status
        FROM web_logs_raw
    """)

    df = pd.DataFrame(rows, columns=["event_time", "user_id", "page", "status"])
    return df.to_json()


def transform(ti):
    df = pd.read_json(ti.xcom_pull())

    metrics = {
        "total_requests": len(df),
        "unique_users": df["user_id"].nunique(),
        "error_rate": (df["status"] >= 400).mean()
    }

    return metrics


def load(ti):
    m = ti.xcom_pull()

    execute_query("""
        INSERT INTO web_metrics
        VALUES (NOW(), %s, %s, %s)
    """, (
        m["total_requests"],
        m["unique_users"],
        m["error_rate"]
    ))

def execute_query(query, params=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params or ())
    conn.commit()
    cur.close()
    conn.close()


def fetch_all(query, params=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params or ())
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
}

with DAG(
    "web_logs_pg",
    description=(
        "Extracts raw web server logs from web_logs_raw, aggregates request "
        "counts, unique users, and error rates, then loads summary metrics "
        "into the web_metrics table. Source: web_logs_raw. Destination: web_metrics."
    ),
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["web", "logs", "etl", "Production",],
    default_args=default_args,
    params={
        "source_table": "web_logs_raw",
        "destination_table": "web_metrics",
        "error_threshold": 400,
    },
    doc_md="""
## Web Logs ETL

### Purpose
High-frequency pipeline that aggregates web server log data into summary
metrics (total requests, unique users, error rate) and persists them for
monitoring and dashboarding.

### Schedule
Runs every **15 minutes**.

### Pipeline
| Step      | Task ID   | Description                                                   |
|-----------|-----------|---------------------------------------------------------------|
| Extract   | extract   | Reads all rows from `web_logs_raw`                            |
| Transform | transform | Aggregates total_requests, unique_users, error_rate (>=400)   |
| Load      | load      | Inserts aggregated snapshot into `web_metrics` with timestamp |

### Source & Destination
- **Source:** `web_logs_raw` (PostgreSQL)
- **Destination:** `web_metrics` (PostgreSQL)

### Metrics Computed
- `total_requests` — row count of the log snapshot
- `unique_users` — distinct user_id count
- `error_rate` — proportion of requests with HTTP status >= 400

### Owner
Team: data-engineering | Alerts: data-engineering-alerts@company.com

### Notes
- Transform output is a dict (not DataFrame); XCom carries raw metrics dict
- No deduplication on load — each run inserts a new timestamped row
""",
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
        doc_md="Reads all rows from `web_logs_raw` and returns as JSON via XCom.",
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
        doc_md="Computes total_requests, unique_users, and error_rate from the log snapshot.",
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
        doc_md="Inserts the aggregated metrics dict into `web_metrics` with current timestamp.",
    )

    extract_task >> transform_task >> load_task