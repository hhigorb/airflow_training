import random
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
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

with DAG(dag_id='branchs',
         default_args=default_args,
         description='airflow_test_dag',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=4, day=30),
         schedule_interval='0 0 * * *',
         catchup=False,
         tags=['airflow_dag']) as dag:

    def generate_random_number():
        return random.randint(1, 100)

    def check_random_number(**kwargs):
        number = kwargs['ti'].xcom_pull(task_ids='generate_random_number_task')
        if number % 2 == 0:
            return 'even_number_task'
        else:
            return 'odd_number_task'

    generate_random_number_task = PythonOperator(
        task_id='generate_random_number_task',
        python_callable=generate_random_number,
        dag=dag
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=check_random_number,
        provide_context=True,
        dag=dag
    )

    even_number_task = BashOperator(
        task_id='even_number_task',
        bash_command='echo "even number"',
        dag=dag
    )

    odd_number_task = BashOperator(
        task_id='odd_number_task',
        bash_command='echo "odd number"',
        dag=dag
    )

    generate_random_number_task >> branch_task
    branch_task >> even_number_task
    branch_task >> odd_number_task
