# dags/utils/alerts.py
"""
Shared failure/success callbacks for all DAGs.
"""

import logging
from utils.db import log_dag_run

logger = logging.getLogger(__name__)


def on_failure_callback(context: dict) -> None:
    """
    Attach to any DAG or task as on_failure_callback=on_failure_callback.
    Logs to dag_run_logs and prints exception info.
    """
    dag_id   = context['dag'].dag_id
    task_id  = context['task_instance'].task_id
    exc      = context.get('exception', 'Unknown error')
    run_id   = context.get('run_id', '')

    logger.error(f"TASK FAILED | DAG: {dag_id} | Task: {task_id} | Error: {exc}")

    try:
        log_dag_run(
            dag_id=dag_id,
            task_id=task_id,
            status='FAILED',
            message=str(exc)[:500]
        )
    except Exception as log_err:
        logger.error(f"Could not write failure log to DB: {log_err}")


def on_success_callback(context: dict) -> None:
    """Log successful task completion."""
    dag_id  = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    logger.info(f"TASK SUCCESS | DAG: {dag_id} | Task: {task_id}")
    try:
        log_dag_run(dag_id=dag_id, task_id=task_id, status='SUCCESS')
    except Exception as log_err:
        logger.error(f"Could not write success log to DB: {log_err}")
