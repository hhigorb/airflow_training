from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

"""
É possível escrever os argumentos a nível de DAG e a nível de task. Ao
escrever os default_args na task, irá sobrescrever os argumentos
do default_args.
"""

doc_md = """
Documentação da DAG
"""

dag_owner = 'data-engineer-team'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'email': ['hhigorb@outlook.com', 'hhigorb@gmail.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5)
                }

with DAG(dag_id='default_args',
         default_args=default_args,
         description='airflow_test_dag',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=4, day=30),
         schedule_interval='@daily',
         catchup=False,
         tags=['airflow_dag']) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command="sleep 5",
        dag=dag
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command="sleep 5",
        dag=dag
    )

    task1 >> task2
