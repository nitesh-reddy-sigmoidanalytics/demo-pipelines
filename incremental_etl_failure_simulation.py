from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import time


# --------------------------------------------------
# 1️⃣ Wait for upstream data partition
# --------------------------------------------------
def check_partition_available():
    available = False
    if not available:
        raise Exception("Partition not available yet")


# --------------------------------------------------
# 2️⃣ Extract from Data Warehouse
# --------------------------------------------------
def extract_from_dw():
    time.sleep(5)
    if random.random() < 0.7:
        raise TimeoutError("Warehouse query timed out")


# --------------------------------------------------
# 3️⃣ Transform Data
# --------------------------------------------------
def transform_data():
    data = {"records": 100}
    value = data["missing_key"]  # KeyError


# --------------------------------------------------
# 4️⃣ Validate Row Count
# --------------------------------------------------
def validate_row_count():
    expected = 1000
    actual = 120

    if actual < expected * 0.5:
        raise ValueError(
            f"Row count anomaly: expected ~{expected}, got {actual}"
        )


# --------------------------------------------------
# 5️⃣ Load to Target System
# --------------------------------------------------
def load_to_target():
    raise PermissionError("Target system authentication failed")


# --------------------------------------------------
# 6️⃣ Final Task
# --------------------------------------------------
def notify_success():
    print("Pipeline completed successfully")


# --------------------------------------------------
# DAG Definition
# --------------------------------------------------
with DAG(
    dag_id="incremental_etl_failure_simulation",
    start_date=datetime(2024, 1, 1),   # ✅ FIXED
    schedule=None,                     # ✅ Modern replacement
    catchup=False,
    tags=["rca_test", "etl_failure"],
) as dag:

    wait_partition = PythonOperator(
        task_id="wait_for_partition",
        python_callable=check_partition_available,
    )

    extract = PythonOperator(
        task_id="extract_from_dw",
        python_callable=extract_from_dw,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    validate = PythonOperator(
        task_id="validate_row_count",
        python_callable=validate_row_count,
    )

    load = PythonOperator(
        task_id="load_to_target",
        python_callable=load_to_target,
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    wait_partition >> extract >> transform >> validate >> load >> notify


# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.sensors.python import PythonSensor
# from airflow.utils.dates import days_ago
# import random
# import time


# # --------------------------------------------------
# # 1️⃣ Wait for upstream data partition
# # --------------------------------------------------
# def check_partition_available():
#     # Simulate missing partition (common real issue)
#     available = False

#     if not available:
#         raise Exception("Partition not available yet")


# # --------------------------------------------------
# # 2️⃣ Extract from Data Warehouse
# # --------------------------------------------------
# def extract_from_dw():
#     # Simulate long-running query timeout
#     time.sleep(5)

#     if random.random() < 0.7:
#         raise TimeoutError("Warehouse query timed out")


# # --------------------------------------------------
# # 3️⃣ Transform Data
# # --------------------------------------------------
# def transform_data():
#     # Simulate transformation bug
#     data = {"records": 100}

#     # Bug: accessing non-existent key
#     value = data["missing_key"]


# # --------------------------------------------------
# # 4️⃣ Validate Row Count
# # --------------------------------------------------
# def validate_row_count():
#     expected = 1000
#     actual = 120  # Simulate anomaly

#     if actual < expected * 0.5:
#         raise ValueError(
#             f"Row count anomaly: expected ~{expected}, got {actual}"
#         )


# # --------------------------------------------------
# # 5️⃣ Load to Target System
# # --------------------------------------------------
# def load_to_target():
#     # Simulate credential expiration
#     raise PermissionError("Target system authentication failed")


# # --------------------------------------------------
# # 6️⃣ Final Task
# # --------------------------------------------------
# def notify_success():
#     print("Pipeline completed successfully")


# # --------------------------------------------------
# # DAG Definition
# # --------------------------------------------------
# with DAG(
#     dag_id="incremental_etl_failure_simulation",
#     start_date=days_ago(1),
#     schedule_interval=None,
#     catchup=False,
#     tags=["rca_test", "etl_failure"],
# ) as dag:

#     wait_partition = PythonOperator(
#         task_id="wait_for_partition",
#         python_callable=check_partition_available,
#     )

#     extract = PythonOperator(
#         task_id="extract_from_dw",
#         python_callable=extract_from_dw,
#     )

#     transform = PythonOperator(
#         task_id="transform_data",
#         python_callable=transform_data,
#     )

#     validate = PythonOperator(
#         task_id="validate_row_count",
#         python_callable=validate_row_count,
#     )

#     load = PythonOperator(
#         task_id="load_to_target",
#         python_callable=load_to_target,
#     )

#     notify = PythonOperator(
#         task_id="notify_success",
#         python_callable=notify_success,
#     )

#     wait_partition >> extract >> transform >> validate >> load >> notify
