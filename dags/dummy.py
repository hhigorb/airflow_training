from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

"""
O airflow não suporta paralelismo duplo, ou seja, não é possível
fazer o seguinte:
[task1, task2, task3] >> [task4, task5]

Pra resolver isso, utilizamos uma task dummy, que não faz nada.
"""

doc_md = """
Documentação da DAG
"""

dag_owner = 'data-engineer-team'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5)
                }

with DAG(dag_id='dummy',
         default_args=default_args,
         description='airflow_test_dag',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=4, day=30),
         schedule_interval='0 0 * * *',
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

    task3 = BashOperator(
        task_id='task3',
        bash_command="sleep 5",
        dag=dag
    )

    task4 = BashOperator(
        task_id='task4',
        bash_command="sleep 5",
        dag=dag
    )

    task5 = BashOperator(
        task_id='task5',
        bash_command="sleep 5",
        dag=dag
    )

    dummy_task = DummyOperator(
        task_id='dummy_task',
        dag=dag
    )

    [task1, task2, task3] >> dummy_task >> [task4, task5]
