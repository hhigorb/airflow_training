import requests
from airflow import DAG
from big_data_operator import BigDataOperator
from datetime import datetime, timedelta

"""
Basicamente com plugins consigo criar novos operadores e hooks para o airflow
Crio uma base e herdo o BaseOperator e o BaseHook
"""

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

with DAG(dag_id='plugins',
         default_args=default_args,
         description='airflow_test_dag',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=4, day=30),
         schedule_interval='@daily',
         catchup=False,
         tags=['airflow_dag']) as dag:

    big_data = BigDataOperator(
        task_id='big_data',
        path_to_csv_file='/opt/airflow/data/Churn.csv',
        path_to_save_file='/opt/airflow/data/Churn.parquet',
        separator=';',
        file_type='parquet',
        dag=dag
    )

    big_data
