import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

# 🔧 Update these credentials
DB_CONFIG = {
    "host": "autopipe.postgres.database.azure.com",
    "port": 5432,
    "dbname": "dags_db",
    "user": "airflow",
    "password": "adminlogin@1"
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# -------------------------
# READ SQL → DataFrame
# -------------------------
def read_sql(query, params=None):
    conn = get_conn()
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df


# -------------------------
# UPSERT DataFrame
# -------------------------
def upsert_dataframe(df, table, conflict_cols):
    conn = get_conn()
    cur = conn.cursor()

    cols = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]

    insert_query = f"""
        INSERT INTO {table} ({','.join(cols)})
        VALUES %s
        ON CONFLICT ({','.join(conflict_cols)})
        DO UPDATE SET
        {', '.join([f"{c}=EXCLUDED.{c}" for c in cols if c not in conflict_cols])}
    """

    execute_values(cur, insert_query, values)

    conn.commit()
    rows = cur.rowcount

    cur.close()
    conn.close()
    return rows


# -------------------------
# OPTIONAL DAG RUN LOGGING
# -------------------------
def log_dag_run(dag_id, task_id, status, rows_processed=0):
    print(f"[LOG] {dag_id}.{task_id} — {status} — rows={rows_processed}")
