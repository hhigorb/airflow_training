import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta

doc_md = """
Documentação da DAG
"""

dag_owner = 'data-engineer-team'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'email': 'hhigorb@outlook.com',
                'email_on_failure': True,
                'email_on_retry': False,
                'retries': None,
                'retry_delay': timedelta(minutes=5)
                }

with DAG(dag_id='sensors',
         default_args=default_args,
         description='airflow_test_dag',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=4, day=30),
         schedule_interval='@daily',
         catchup=False,
         tags=['airflow_dag']) as dag:

    def query_api():
        response = requests.get('https://api.publicapis.org/entries')
        print(response.text)

    check_api = HttpSensor(
        task_id='check_api',
        http_conn_id='http_conn',
        endpoint='entries',
        poke_interval=5,
        timeout=60,
        dag=dag
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=query_api,
        dag=dag

    )

    check_api >> process_data
