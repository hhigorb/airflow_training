from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup

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

with DAG(dag_id='taskgroup',
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

    task6 = BashOperator(
        task_id='task6',
        bash_command="sleep 5",
        dag=dag
    )

    task_group = TaskGroup(
        group_id='task_group',
        dag=dag)

    task7 = BashOperator(
        task_id='task7',
        bash_command="sleep 5",
        dag=dag,
        task_group=task_group
    )

    task8 = BashOperator(
        task_id='task8',
        bash_command="sleep 5",
        dag=dag,
        task_group=task_group
    )

    task9 = BashOperator(
        task_id='task9',
        bash_command="sleep 5",
        dag=dag,
        trigger_rule='one_failed',
        task_group=task_group
    )

    task1 >> task2
    task3 >> task4
    [task2, task4] >> task5 >> task6
    task6 >> task_group
