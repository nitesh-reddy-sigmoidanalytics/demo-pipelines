# dags/etl_sales_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

from utils.db import read_sql, upsert_dataframe, log_dag_run
from utils.validators import (validate_required_columns, validate_null_threshold,
                               validate_row_count, validate_no_negatives)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-team',
    'retries': 1,
}

def extract_sales(**context):
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')           # ✅ Fixed Bug 1

    df = read_sql(
        "SELECT id, sale_date, product_id, customer_id, quantity, unit_price, region FROM raw_sales WHERE sale_date = %s",
        params=(date_str,)
    )

    validate_required_columns(df, ['id','sale_date','quantity','unit_price','region'], 'extract_sales')
    validate_row_count(df, min_rows=0, context_name='extract_sales')
    validate_no_negatives(df, ['quantity', 'unit_price'], 'extract_sales')

    context['ti'].xcom_push(key='raw_sales', value=df.to_json())
    log_dag_run('etl_sales_pipeline', 'extract_sales', 'SUCCESS', rows_processed=len(df))
    return f"Extracted {len(df)} records for {date_str}"


def transform_sales(**context):
    raw_json = context['ti'].xcom_pull(key='raw_sales', task_ids='extract_sales')
    df = pd.read_json(raw_json)

    df['revenue'] = df['quantity'] * df['unit_price']        # ✅ Fixed Bug 2

    summary = df.groupby(['sale_date', 'region']).agg(
        total_revenue=('revenue', 'sum'),
        total_orders=('id', 'count'),
    ).reset_index()

    summary['avg_order_value'] = (                           # ✅ Fixed Bug 3
        summary['total_revenue'] / summary['total_orders']
    )
    summary.rename(columns={'sale_date': 'summary_date'}, inplace=True)

    validate_null_threshold(summary, threshold=0.05, context_name='transform_sales')

    context['ti'].xcom_push(key='sales_summary', value=summary.to_json())
    log_dag_run('etl_sales_pipeline', 'transform_sales', 'SUCCESS', rows_processed=len(summary))
    return f"Transformed {len(summary)} summary rows"


def load_sales(**context):
    summary_json = context['ti'].xcom_pull(key='sales_summary', task_ids='transform_sales')
    summary = pd.read_json(summary_json)

    rows = upsert_dataframe(                                  # ✅ Fixed Bug 4
        df=summary,
        table='sales_summary',
        conflict_cols=['summary_date', 'region']
    )

    log_dag_run('etl_sales_pipeline', 'load_sales', 'SUCCESS', rows_processed=rows)
    return f"Upserted {rows} rows into sales_summary"


with DAG(
    dag_id='etl_sales_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 15),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'sales']
) as dag:

    extract   = PythonOperator(task_id='extract_sales',   python_callable=extract_sales)
    transform = PythonOperator(task_id='transform_sales', python_callable=transform_sales)
    load      = PythonOperator(task_id='load_sales',      python_callable=load_sales)

    extract >> transform >> load
