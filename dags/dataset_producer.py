import pandas as pd
from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

doc_md = """
Documentação da DAG
"""

dag_owner = 'data-engineer-team'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'retries': None,
                'retry_delay': timedelta(minutes=5)
                }

with DAG(dag_id='producer',
         default_args=default_args,
         description='airflow_test_dag',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=4, day=30),
         schedule_interval='0 0 * * *',
         catchup=False,
         tags=['airflow_dag']) as dag:

    my_dataset = Dataset('/opt/airflow/data/Churn_new.csv')

    def my_file():
        dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
        dataset.to_csv('/opt/airflow/data/Churn_new.csv', sep=';')

    task1 = PythonOperator(
        task_id='task1',
        python_callable=my_file,
        dag=dag,
        outlets=[my_dataset]
    )
