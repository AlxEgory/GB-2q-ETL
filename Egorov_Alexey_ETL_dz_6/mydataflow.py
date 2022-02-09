from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import random


def dump_data(**kwargs):
    phook = PostgresHook(postgres_conn_id="postgres_source")
    conn = phook.get_conn()
    with conn.cursor() as cursor:
        customer = "COPY customer TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open('customer.csv', 'w') as f:
            cursor.copy_expert(customer, f)
        lineitem = "COPY lineitem TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open('lineitem.csv', 'w') as f:
            cursor.copy_expert(lineitem, f)
        orders = "COPY orders TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open('orders.csv', 'w') as f:
            cursor.copy_expert(orders, f)
        partsupp = "COPY partsupp TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open('partsupp.csv', 'w') as f:
            cursor.copy_expert(partsupp, f)
        supplier = "COPY supplier TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open('supplier.csv', 'w') as f:
            cursor.copy_expert(supplier, f)
        part = "COPY part TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open('part.csv', 'w') as f:
            cursor.copy_expert(part, f)
        region = "COPY region TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open('region.csv', 'w') as f:
            cursor.copy_expert(region, f)
        nation = "COPY nation TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open('nation.csv', 'w') as f:
            cursor.copy_expert(nation, f)


def import_data(**kwargs):
    phook = PostgresHook(postgres_conn_id="postgres_target")
    conn = phook.get_conn()
    with conn.cursor() as cursor:
        customer = "COPY customer from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open('customer.csv', 'r') as f:
            cursor.copy_expert(customer, f)
        lineitem = "COPY lineitem from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open('lineitem.csv', 'r') as f:
            cursor.copy_expert(lineitem, f)
        orders = "COPY orders from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open('orders.csv', 'r') as f:
            cursor.copy_expert(orders, f)
        partsupp = "COPY partsupp from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open('partsupp.csv', 'r') as f:
            cursor.copy_expert(partsupp, f)
        supplier = "COPY supplier from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open('supplier.csv', 'r') as f:
            cursor.copy_expert(supplier, f)
        part = "COPY part from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open('part.csv', 'r') as f:
            cursor.copy_expert(part, f)
        region = "COPY region from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open('region.csv', 'r') as f:
            cursor.copy_expert(region, f)
        nation = "COPY nation from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open('nation.csv', 'r') as f:
            cursor.copy_expert(nation, f)


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2022, 2, 9),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

with DAG(
        dag_id="import_dump4",
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        tags=['data-flow'],
        catchup=False
) as dag:
    dump_my_data = PythonOperator(
        task_id='dump_my_data',
        python_callable=dump_data
    )

    import_my_data = PythonOperator(
        task_id='import_my_data',
        python_callable=import_data
    )

    dump_my_data >> import_my_data
