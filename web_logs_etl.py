from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from utils.db import execute_query

RAW_PATH = "/opt/airflow/data/web_logs.json"

def extract():
    df = pd.read_json(RAW_PATH)
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
        VALUES (NOW, %s, %s, %s)
    """, (m["total_requests"], m["unique_users"], m["error_rate"]))

with DAG(
    "web_logs_pg",
    start_date=datetime(2026, 2, 1),
    catchup=False,
) as dag:

    PythonOperator(task_id="extract", python_callable=extract) >> \
    PythonOperator(task_id="transform", python_callable=transform) >> \
    PythonOperator(task_id="load", python_callable=load)
