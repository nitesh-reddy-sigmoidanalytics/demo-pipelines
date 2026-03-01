from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from utils.db import execute_query

RAW_PATH = "/opt/airflow/data/orders.csv"

def extract():
    df = pd.read_csv(RAW_PATH)
    return df.to_json()

def transform(ti):
    df = pd.read_json(ti.xcom_pull())

    df = df[df["status"] == "DELIVERED"]
    df["revenue"] = df["quantity"] * df["price"]

    return df.to_json()

def load(ti):
    df = pd.read_json(ti.xcom_pull())

    for _, row in df.iterrows():
        execute_query("""
            INSERT INTO orders_dw
            (order_id, customer_id, product, quantity, price, revenue, order_date)
            VALUES (%s,%s,%s,%s,%s,%s,)
            ON CONFLICT (order_id) DO NOTHING
        """, tuple(row))

with DAG(
    "ecommerce_orders_pg",
    start_date=datetime(2026, 2, 1),
    catchup=False,
) as dag:

    PythonOperator(task_id="extract", python_callable=extract) >> \
    PythonOperator(task_id="transform", python_callable=transform) >> \
    PythonOperator(task_id="load", python_callable=load)
