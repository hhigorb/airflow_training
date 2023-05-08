from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
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

with DAG(dag_id='exemplo_xcom',
         default_args=default_args,
         description='airflow_test_dag',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=4, day=30),
         schedule_interval='0 0 * * *',
         catchup=False,
         tags=['airflow_dag']) as dag:

    def task_write(**kwargs):
        kwargs['ti'].xcom_push(key='valorxcom1', value=10)

    def task_read(**kwargs):
        valor = kwargs['ti'].xcom_pull(key='valorxcom1')
        print(f'Valor recuperado: {valor}')

    task1 = PythonOperator(
        task_id='task1',
        python_callable=task_write,
        dag=dag
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=task_read,
        dag=dag
    )

    task1 >> task2
