from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta

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

with DAG(dag_id='envio_email',
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

    task3 = BashOperator(
        task_id='task3',
        bash_command="sleep 5",
        dag=dag
    )

    task4 = BashOperator(
        task_id='task4',
        bash_command="exit 1",
        dag=dag
    )

    task5 = BashOperator(
        task_id='task5',
        bash_command="sleep 5",
        trigger_rule='none_failed',
        dag=dag
    )

    task6 = BashOperator(
        task_id='task6',
        bash_command="sleep 5",
        trigger_rule='none_failed',
        dag=dag
    )

    send_email = EmailOperator(
        task_id='send_email',
        to='hhigorb@outlook.com',
        subject='Airflow Error',
        html_content="""
                     <h3>Ocorreu um erro na dag.</h3>
                     <p>Dag: envio_email</p>
                    """,
        dag=dag,
        trigger_rule='one_failed'
    )

    [task1, task2] >> task3 >> task4
    task4 >> [task5, task6, send_email]
