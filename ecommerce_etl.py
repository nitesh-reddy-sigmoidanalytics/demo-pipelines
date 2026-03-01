from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
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
    df = pd.read_json(ti.xcom_pull())

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
            VALUES (%s,%s,%s,%s,%s,%s,)
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


with DAG(
    "ecommerce_orders_pg",
    start_date=datetime(2026, 2, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    extract_task >> transform_task >> load_task
