from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from utils.db import get_conn


def extract():
    rows = fetch_all("""
        SELECT order_id, customer_id, product,
               quantity, price, status, order_date
        FROM orders_raw
    """)

    df = pd.DataFrame(
        rows,
        columns=[
            "order_id", "customer_id", "product",
            "quantity", "price", "status", "order_date"
        ],
    )

    return df.to_json()


def transform(ti):
    input_data = ti.xcom_pull()
    if input_data is None:
        raise ValueError("No data received for transformation.")
    df = pd.read_json(input_data)

    # Keep only delivered orders
    df = df[df["status"] == "DELIVERED"]

    # Calculate revenue
    df["revenue"] = df["quantity"] * df["price"]

    return df.to_json()


def load(ti):
    df = pd.read_json(ti.xcom_pull())

    for _, r in df.iterrows():
        execute_query("""
            INSERT INTO orders_dw
            (order_id, customer_id, product,
             quantity, price, revenue, order_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (order_id) DO NOTHING
        """, (
            r.order_id,
            r.customer_id,
            r.product,
            r.quantity,
            r.price,
            r.revenue,
            r.order_date,
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
    "ecommerce_orders_pg",
    description=(
        "Extracts raw orders from PostgreSQL orders_raw table, filters to DELIVERED "
        "status, calculates revenue per order, and loads into orders_dw data warehouse. "
        "Source: orders_raw. Destination: orders_dw."
    ),
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["ecommerce", "orders", "etl", "postgresql", "daily"],
    default_args=default_args,
    params={
        "source_table": "orders_raw",
        "destination_table": "orders_dw",
        "filter_status": "DELIVERED",
    },
    doc_md="""
## Ecommerce Orders ETL

### Purpose
Full ETL pipeline that moves delivered orders from the raw PostgreSQL table
into the data warehouse with revenue calculated.

### Schedule
Runs daily at **03:00 UTC**.

### Pipeline
| Step      | Task ID   | Description                                      |
|-----------|-----------|--------------------------------------------------|
| Extract   | extract   | Reads all rows from `orders_raw`                 |
| Transform | transform | Filters DELIVERED orders, computes revenue field |
| Load      | load      | Upserts into `orders_dw` (conflict on order_id)  |

### Source & Destination
- **Source:** `orders_raw` (PostgreSQL)
- **Destination:** `orders_dw` (PostgreSQL)

### Owner
Team: data-engineering | Alerts: data-engineering-alerts@company.com

### Notes
- Uses XCom to pass DataFrames between tasks as JSON
- Idempotent load via `ON CONFLICT (order_id) DO NOTHING`
""",
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
        doc_md="Fetches all rows from `orders_raw` and returns as JSON via XCom.",
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
        doc_md="Filters to DELIVERED orders only and calculates `revenue = quantity * price`.",
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
        doc_md="Inserts transformed rows into `orders_dw`. Skips duplicates on `order_id`.",
    )

    extract_task >> transform_task >> load_task