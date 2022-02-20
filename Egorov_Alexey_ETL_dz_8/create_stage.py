from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import random

tables = ['orders', 'products', 'suppliers', 'orderdetails', 'productsuppl']

def dump_data(**kwargs):
    phook = PostgresHook(postgres_conn_id="postgres_source")
    conn = phook.get_conn()
    for table in tables:
        with conn.cursor() as cursor:
            query = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
            with open(f'{table}.csv', 'w') as f:
                cursor.copy_expert(query, f)

def import_data(**kwargs):
    phook = PostgresHook(postgres_conn_id="postgres_stage")
    conn = phook.get_conn()
    for table in tables:
        with conn.cursor() as cursor:
            query = f"COPY {table} from STDIN WITH DELIMITER ',' CSV HEADER;"
            with open(f'{table}.csv', 'r') as f:
                cursor.copy_expert(query, f)
            conn.commit()

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2022, 2, 20),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

with DAG(
        dag_id="import_stage",
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        tags=['data-flow'],
        catchup=False
) as dag:
    dump_my_data = PythonOperator(
        task_id='dump_my_data',
        python_callable=dump_data
    )

    import_to_stage = PythonOperator(
        task_id='import_to_stage',
        python_callable=import_data
    )

    dump_my_data >> import_to_stage
