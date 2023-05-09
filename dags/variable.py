from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
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

with DAG(dag_id='variable',
         default_args=default_args,
         description='airflow_test_dag',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=4, day=30),
         schedule_interval='0 0 * * *',
         catchup=False,
         tags=['airflow_dag']) as dag:

    def print_variable(**kwargs):
        var = Variable.get('my_variable')
        print(f'O valor da variável é: {var}')

    task1 = PythonOperator(
        task_id='task1',
        python_callable=print_variable,
        dag=dag
    )

    task1
