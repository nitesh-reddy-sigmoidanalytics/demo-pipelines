import psycopg2

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="warehouse",
        user="airflow",
        password="airflow",
        port=5433           # Correct mapped port
    )
# def get_db_connection():
#     return psycopg2.connect(
#         host="postgres-postgresql",
#         database="warehouse",
#         user="airflow",
#         password="airflow",
#         port=5432           # Correct mapped port
#     )

def insert_records(table, records):
    conn = get_db_connection()
    cur = conn.cursor()
    for r in records:
        cur.execute(
            f"INSERT INTO {table} VALUES (%s, %s, %s)",
            tuple(r.values())
        )
    conn.commit()
    cur.close()
    conn.close()
