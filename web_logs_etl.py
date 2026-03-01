from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
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

with DAG(
    "web_logs_pg",
    start_date=datetime(2026, 2, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task