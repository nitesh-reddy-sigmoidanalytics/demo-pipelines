from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

from utils.db import read_sql, upsert_dataframe

logger = logging.getLogger(__name__)

default_args = {
    "owner": "analytics",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
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

    # 🔥 BUG 1 — timezone-aware vs naive datetime mismatch
    orders["order_ts"] = pd.to_datetime(orders["order_ts"], utc=True)
    customers["signup_date"] = pd.to_datetime(customers["signup_date"])

    # 🔥 BUG 2 — merge creates duplicated columns silently
    merged = pd.merge(orders, customers, on="customer_id", how="left")

    # 🔥 BUG 3 — cohort calculation fails due to dtype mismatch
    merged["cohort_days"] = (
        merged["order_ts"].dt.date - merged["signup_date"]
    ).dt.days

    # 🔥 BUG 4 — aggregation assumes no nulls
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
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["analytics", "revenue"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders,
    )

    t2 = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers,
    )

    t3 = PythonOperator(
        task_id="transform_revenue",
        python_callable=transform_revenue,
    )

    t4 = PythonOperator(
        task_id="load_revenue",
        python_callable=load_revenue,
    )

    [t1, t2] >> t3 >> t4
