from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import random




def dump_data(**kwargs):
    phook = PostgresHook(postgres_conn_id="postgres_stage")
    conn = phook.get_conn()
    with conn.cursor() as cursor:
        query = "COPY (SELECT orderid FROM orders) TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open('h_orders.csv', 'w') as f:
            cursor.copy_expert(query, f)
        query = "COPY (SELECT orderdate, orderstatus, orderpriority, clerk FROM orders) TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open('s_orders.csv', 'w') as f:
            cursor.copy_expert(query, f)
        # не разобрался, как правильно написать выгрузку для stage. При загрузке в core выдает ошибку. Помогите пжлст.

def import_data(**kwargs):
    tables = ['h_orders', 's_orders', 'h_products', 's_products', 'h_suppliers', 's_suppliers', 'l_orderdetails', 's_orderdetails', 'l_productsuppl', 's_productsuppl']
    phook = PostgresHook(postgres_conn_id="postgres_core")
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
        dag_id="create_stage2",
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        tags=['data-flow'],
        catchup=False
) as dag:
    dump_stage = PythonOperator(
        task_id='dump_stage',
        python_callable=dump_data
    )

    import_to_core = PythonOperator(
        task_id='import_to_core',
        python_callable=import_data
    )

    dump_stage >> import_to_core
