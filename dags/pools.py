from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

doc_md = """
Documentação da DAG
"""

dag_owner = 'data-engineer-team'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5)
                }

with DAG(dag_id='pool',
         default_args=default_args,
         description='airflow_test_dag',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=4, day=30),
         schedule_interval='0 0 * * *',
         catchup=False,
         tags=['airflow_dag']) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='sleep 5',
        dag=dag,
        pool='mypool'
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='sleep 5',
        dag=dag,
        pool='mypool',
        priority_weight=5
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command='sleep 5',
        dag=dag,
        pool='mypool'
    )

    task4 = BashOperator(
        task_id='task4',
        bash_command='sleep 5',
        dag=dag,
        pool='mypool',
        priority_weight=10
    )
