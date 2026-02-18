from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from common.db_utils import get_db_connection

def cleanup_data():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM sales WHERE sale_date < CURRENT_DATE - INTERVAL 90 days")
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    "cleanup_old_data",
    start_date=datetime(2024,1,1),
    schedule="@weekly",
    catchup=False
) as dag:
    PythonOperator(
        task_id="cleanup_data",
        python_callable=cleanup_data
    )
