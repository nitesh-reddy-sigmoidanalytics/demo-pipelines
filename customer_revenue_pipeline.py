from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

from utils.db import read_sql, upsert_dataframe

logger = logging.getLogger(__name__)

default_args = {
    "owner": "analytics",
    "depends_on_past": False,
}

# ---------------------------------------------------
# TASK 1 — Extract Orders
# ---------------------------------------------------
def extract_orders(**context):
    execution_date = context["execution_date"]

    orders = read_sql(
        """
        SELECT order_id, customer_id, order_ts, amount
        FROM raw_orders
        WHERE DATE(order_ts) = %s
        """,
        params=(execution_date.strftime("%Y-%m-%d"),),
    )

    context["ti"].xcom_push(key="orders", value=orders.to_json())


# ---------------------------------------------------
# TASK 2 — Extract Customers
# ---------------------------------------------------
def extract_customers(**context):
    customers = read_sql(
        """
        SELECT customer_id, signup_date, country
        FROM customers
        """
    )

    context["ti"].xcom_push(key="customers", value=customers.to_json())


# ---------------------------------------------------
# TASK 3 — Transform Revenue (FAILS HERE)
# ---------------------------------------------------
def transform_revenue(**context):
    orders = pd.read_json(
        context["ti"].xcom_pull(key="orders", task_ids="extract_orders")
    )
    customers = pd.read_json(
        context["ti"].xcom_pull(key="customers", task_ids="extract_customers")
    )


    orders["order_ts"] = pd.to_datetime(orders["order_ts"], utc=True)
    customers["signup_date"] = pd.to_datetime(customers["signup_date"])


    merged = pd.merge(orders, customers, on="customer_id", how="left")

    merged["cohort_days"] = (
        merged["order_ts"].dt.date - merged["signup_date"]
    ).dt.days


    summary = merged.groupby(["country"]).agg(
        total_revenue=("amount", "sum"),
        avg_cohort=("cohort_days", "mean"),
        orders=("order_id", "count"),
    ).reset_index()

    context["ti"].xcom_push(key="summary", value=summary.to_json())


# ---------------------------------------------------
# TASK 4 — Load to Warehouse
# ---------------------------------------------------
def load_revenue(**context):
    summary = pd.read_json(
        context["ti"].xcom_pull(key="summary", task_ids="transform_revenue")
    )

    rows = upsert_dataframe(
        summary,
        table="customer_revenue_daily",
        conflict_cols=["country"],
    )

    logger.info("Upserted %s rows", rows)


# ---------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------
with DAG(
    dag_id="customer_revenue_pipeline",
    description=(
        "Daily analytics pipeline that joins orders and customer data to compute "
        "per-country revenue summaries and cohort metrics. Processes one day of "
        "orders per run (execution_date partitioned). "
        "Sources: raw_orders, customers. Destination: customer_revenue_daily."
    ),
    schedule_interval="0 6 * * *",         # Daily at 6:00 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args=default_args,
    tags=["dev", "revenue", "customers", ],
    params={
        "source_orders_table": "raw_orders",
        "source_customers_table": "customers",
        "destination_table": "customer_revenue_daily",
        "conflict_cols": ["country"],
    },
    doc_md="""
## Customer Revenue Pipeline

### Purpose
Daily analytics pipeline that computes per-country revenue summaries by joining
orders (partitioned by `execution_date`) with the full customers dimension table.
Outputs cohort metrics and total revenue into `customer_revenue_daily`.

### Schedule
Runs daily at **06:00 UTC**. Processes orders for the previous execution date.

### Pipeline
| Step              | Task ID           | Description                                                        |
|-------------------|-------------------|--------------------------------------------------------------------|
| Extract Orders    | extract_orders    | Reads orders from `raw_orders` filtered by execution_date          |
| Extract Customers | extract_customers | Reads full `customers` dimension (customer_id, signup_date, country)|
| Transform Revenue | transform_revenue | Joins orders + customers, computes cohort_days and revenue by country|
| Load Revenue      | load_revenue      | Upserts country-level summary into `customer_revenue_daily`        |

### Dependency Graph
```
extract_orders ──┐
                 ├──► transform_revenue ──► load_revenue
extract_customers┘
```

### Source & Destination
- **Sources:** `raw_orders` (partitioned by order_ts), `customers` (full scan)
- **Destination:** `customer_revenue_daily` (upsert on `country`)

### XCom Keys
| Key       | Producer          | Consumer          | Content                    |
|-----------|-------------------|-------------------|----------------------------|
| orders    | extract_orders    | transform_revenue | JSON DataFrame of orders   |
| customers | extract_customers | transform_revenue | JSON DataFrame of customers|
| summary   | transform_revenue | load_revenue      | JSON DataFrame of summary  |

### Known Issues
- `transform_revenue` has known bugs around timezone mismatch, null handling,
  and dtype mismatches in cohort calculation — see inline comments in task code.

### Owner
Team: analytics | Alerts: analytics-alerts@company.com
""",
) as dag:

    t1 = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders,
        doc_md=(
            "Reads orders from `raw_orders` for the DAG execution date. "
            "Pushes result to XCom key `orders`."
        ),
    )

    t2 = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers,
        doc_md=(
            "Full scan of `customers` table (customer_id, signup_date, country). "
            "Pushes result to XCom key `customers`."
        ),
    )

    t3 = PythonOperator(
        task_id="transform_revenue",
        python_callable=transform_revenue,
        doc_md=(
            "Joins orders and customers on customer_id. Computes cohort_days and "
            "aggregates total_revenue, avg_cohort, and order count by country. "
            "Pushes result to XCom key `summary`."
        ),
    )

    t4 = PythonOperator(
        task_id="load_revenue",
        python_callable=load_revenue,
        doc_md=(
            "Upserts country-level revenue summary into `customer_revenue_daily`. "
            "Conflict resolution on `country` column."
        ),
    )

    [t1, t2] >> t3 >> t4
