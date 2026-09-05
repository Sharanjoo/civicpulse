from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
import pandas as pd
from contextlib import contextmanager
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

@contextmanager
def pg_cursor():
    """Yields (conn, cursor) for postgres_default; commits on success,
    rolls back on failure, and always closes the connection so a retry
    starts from a clean state instead of leaking a connection."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        yield conn, cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def create_tables():
    with pg_cursor() as (conn, cursor):
        cursor.execute("""CREATE TABLE IF NOT EXISTS dim_date (
            date_key INT PRIMARY KEY,
            calendar_date DATE,
            year INT,
            month INT,
            month_name TEXT,
            day INT,
            day_of_week INT,
            day_name TEXT,
            week_start_date DATE,
            week_of_year INT,
            quarter INT,
            is_weekend BOOLEAN,
            academic_year TEXT,
            semester TEXT,
            is_instruction_day BOOLEAN,
            is_holiday BOOLEAN,
            is_recess BOOLEAN,
            is_exam_period BOOLEAN,
            is_study_day BOOLEAN,
            academic_event_name TEXT,
            academic_event_type TEXT,
            library_hours_text TEXT,
            library_open_time TEXT,
            library_close_time TEXT,
            library_hours_open FLOAT,
            is_library_closed BOOLEAN
        );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS dim_time (
            time_key INT PRIMARY KEY,
            time_value TEXT,
            hour_24 INT,
            hour_12 INT,
            minute INT,
            am_pm TEXT,
            time_label TEXT,
            part_of_day TEXT,
            is_morning BOOLEAN,
            is_afternoon BOOLEAN,
            is_evening BOOLEAN,
            is_late_night BOOLEAN
        );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS dim_room (
            room_key INT PRIMARY KEY,
            item_id TEXT,
            room_name TEXT,
            category_name TEXT,
            category_url TEXT,
            location_name TEXT,
            seat_name TEXT,
            room_label TEXT,
            is_study_room BOOLEAN
        );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS fact_bookings (
            booking_key INT PRIMARY KEY,
            date_key INT,
            start_time_key INT,
            end_time_key INT,
            room_key INT,
            booking_name TEXT,
            start_datetime TIMESTAMP,
            end_datetime TIMESTAMP,
            duration_minutes INT,
            duration_hours FLOAT,
            booking_count INT
        );""")

        cursor.execute("""
            ALTER TABLE fact_bookings
            DROP CONSTRAINT IF EXISTS fk_fact_date,
            DROP CONSTRAINT IF EXISTS fk_fact_start_time,
            DROP CONSTRAINT IF EXISTS fk_fact_end_time,
            DROP CONSTRAINT IF EXISTS fk_fact_room;
        """)

        cursor.execute("""
            ALTER TABLE fact_bookings
            ADD CONSTRAINT fk_fact_date
            FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
            ON DELETE CASCADE;
        """)

        cursor.execute("""
            ALTER TABLE fact_bookings
            ADD CONSTRAINT fk_fact_start_time
            FOREIGN KEY (start_time_key) REFERENCES dim_time(time_key)
            ON DELETE CASCADE;
        """)

        cursor.execute("""
            ALTER TABLE fact_bookings
            ADD CONSTRAINT fk_fact_end_time
            FOREIGN KEY (end_time_key) REFERENCES dim_time(time_key)
            ON DELETE CASCADE;
        """)

        cursor.execute("""
            ALTER TABLE fact_bookings
            ADD CONSTRAINT fk_fact_room
            FOREIGN KEY (room_key) REFERENCES dim_room(room_key)
            ON DELETE CASCADE;
        """)

def clean_df(df):
    return df.where(pd.notnull(df), None)

def validate_data():
    with pg_cursor() as (conn, cursor):
        print("=== DATA QUALITY CHECKS ===")

        cursor.execute("SELECT COUNT(*) FROM dim_date;")
        print("dim_date rows:", cursor.fetchone())

        cursor.execute("""
            SELECT COUNT(*)
            FROM fact_bookings f
            LEFT JOIN dim_date d ON f.date_key = d.date_key
            WHERE d.date_key IS NULL;
        """)
        print("orphan date keys:", cursor.fetchone())

        cursor.execute("""
            SELECT COUNT(*)
            FROM fact_bookings f
            LEFT JOIN dim_room r ON f.room_key = r.room_key
            WHERE r.room_key IS NULL;
        """)
        print("orphan room keys:", cursor.fetchone())

def load_dim_date():
    df = pd.read_csv("/opt/airflow/project/warehouse_output/dim_date.csv")
    df = clean_df(df)

    df["library_hours_open"] = pd.to_numeric(
        df["library_hours_open"],
        errors="coerce"
    )

    bool_cols = [
        "is_weekend", "is_instruction_day", "is_holiday",
        "is_recess", "is_exam_period", "is_study_day",
        "is_library_closed"
    ]

    for c in bool_cols:
        df[c] = df[c].map(lambda x: None if pd.isna(x) else bool(x))

    with pg_cursor() as (conn, cursor):
        cursor.execute("""
            TRUNCATE TABLE fact_bookings, dim_room, dim_time, dim_date
            RESTART IDENTITY CASCADE;
        """)

        execute_values(cursor, """
            INSERT INTO dim_date VALUES %s
            ON CONFLICT (date_key) DO NOTHING
        """, df.values.tolist())

def load_dim_time():
    df = pd.read_csv("/opt/airflow/project/warehouse_output/dim_time.csv")
    df = clean_df(df)

    bool_cols = ["is_morning", "is_afternoon", "is_evening", "is_late_night"]
    for c in bool_cols:
        df[c] = df[c].map(lambda x: None if pd.isna(x) else bool(x))

    with pg_cursor() as (conn, cursor):
        execute_values(cursor, """
            INSERT INTO dim_time VALUES %s
            ON CONFLICT (time_key) DO NOTHING
        """, df.values.tolist())

def load_dim_room():
    df = pd.read_csv("/opt/airflow/project/warehouse_output/dim_room.csv")
    df = clean_df(df)

    with pg_cursor() as (conn, cursor):
        execute_values(cursor, """
            INSERT INTO dim_room VALUES %s
            ON CONFLICT (room_key) DO NOTHING
        """, df.values.tolist())

def load_fact_bookings():
    df = pd.read_csv("/opt/airflow/project/warehouse_output/fact_bookings.csv")
    df = clean_df(df)

    with pg_cursor() as (conn, cursor):
        cursor.execute("TRUNCATE TABLE fact_bookings;")

        execute_values(cursor, """
            INSERT INTO fact_bookings VALUES %s
            ON CONFLICT (booking_key) DO NOTHING
        """, df.values.tolist())

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="civicpulse_mlk_library_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 26),
    schedule="0 0 * * *",
    catchup=False,
    tags=["civicpulse", "data226", "etl", "warehouse"],
) as dag:

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables
    )

    extract_bookings = BashOperator(
        task_id="extract_bookings",
        bash_command='python "/opt/airflow/project/code_files/Bookings.py"',
    )

    extract_hours = BashOperator(
        task_id="extract_hours",
        bash_command='python "/opt/airflow/project/code_files/Hours.py"',
    )

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command='python "/opt/airflow/project/code_files/Transform.py"',
    )

    load_dim_date_task = PythonOperator(
        task_id="load_dim_date",
        python_callable=load_dim_date
    )

    load_dim_time_task = PythonOperator(
        task_id="load_dim_time",
        python_callable=load_dim_time
    )

    load_dim_room_task = PythonOperator(
        task_id="load_dim_room",
        python_callable=load_dim_room
    )

    load_fact_bookings_task = PythonOperator(
        task_id="load_fact_bookings",
        python_callable=load_fact_bookings
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    create_tables_task >> extract_bookings >> extract_hours >> transform_data

    transform_data >> load_dim_date_task
    load_dim_date_task >> load_dim_time_task
    load_dim_time_task >> load_dim_room_task
    load_dim_room_task >> load_fact_bookings_task
    load_fact_bookings_task >> validate_task