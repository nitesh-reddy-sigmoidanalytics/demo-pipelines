from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.task_group import TaskGroup
from airflow.utils import timezone
from platform_lib.db import get_connection
from platform_lib.validation import validate_records
from platform_lib.metrics import emit_metric



import json
import logging
from datetime import timedelta

LOG = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-platform",
}


def extract_users(**context):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT id, email FROM users WHERE active = true")
    rows = cur.fetchall()

    records = [{"id": r[0], "email": r[1]} for r in rows]

    context["ti"].xcom_push(key="users", value=records)

    emit_metric("users_extracted", len(records))


def validate_users(**context):
    records = context["ti"].xcom_pull(key="users")

    try:
        validate_records(records)
    except Exception as e:
        emit_metric("user_validation_failed", 1)
        raise AirflowFailException(str(e))

    emit_metric("users_validated", len(records))


def load_users(**context):
    records = context["ti"].xcom_pull(key="users")

    with open("/tmp/users.json", "w") as f:
        json.dump(records, f)

    emit_metric("users_loaded", len(records))


def publish_metrics(**context):
    emit_metric(
        "pipeline_success",
        1,
        tags={"dag": context["dag"].dag_id},
    )


with DAG(
    dag_id="user_data_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=timezone.utcnow() - timedelta(days=1),
    schedule_interval="* * 1 * *",
    catchup=False,
    tags=["users", "platform"],
) as dag:

    with TaskGroup("extract") as extract_group:
        extract_task = PythonOperator(
            task_id="extract_users",
            python_callable=extract_users,
        )

    with TaskGroup("validate") as validate_group:
        validate_task = PythonOperator(
            task_id="validate_users",
            python_callable=validate_users,
        )

    with TaskGroup("load") as load_group:
        load_task = PythonOperator(
            task_id="load_users",
            python_callable=load_users,
        )

    publish_task = PythonOperator(
        task_id="publish_metrics",
        python_callable=publish_metrics,
        trigger_rule="all_success",
    )

    extract_group >> validate_group >> load_group >> publish_task
