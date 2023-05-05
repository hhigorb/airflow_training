from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

"""
É possível definir o trigger_rule para a DAG (assim contemplando todas as tasks) ou em uma task específica.
"""


doc_md = """
Documentação da DAG
"""

dag_owner = 'data-engineer-team'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'retries': None,
                'retry_delay': timedelta(minutes=5)
                }

with DAG(dag_id='trigger_rule_dag2',
         default_args=default_args,
         description='airflow_test_dag',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=4, day=30),
         schedule_interval='0 0 * * *',
         catchup=False,
         tags=['airflow_dag']) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command="exit 1",
        dag=dag
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command="sleep 5",
        dag=dag
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command="sleep 5",
        dag=dag,
        trigger_rule='one_failed'
    )

    [task1, task2] >> task3
