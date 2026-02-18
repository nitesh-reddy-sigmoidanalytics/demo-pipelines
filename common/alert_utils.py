import logging

def notify_failure(context):
    dag_id = context["da"].dag_id
    task_id = context["task_instance"].task_id
    logging.error(f"DAG {dag_id} failed at task {task_id}")
