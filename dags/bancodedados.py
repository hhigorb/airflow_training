import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
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


with DAG(dag_id='pg_database',
         default_args=default_args,
         description='airflow_test_dag',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=4, day=30),
         schedule_interval='@daily',
         catchup=False,
         tags=['airflow_dag']) as dag:

    create_table_query = """
    CREATE TABLE IF NOT EXISTS public.teste (
        id int
    );
    """

    insert_data = """
    INSERT INTO public.teste (id) VALUES (1);
    """

    query_data = """
    SELECT * FROM public.teste;
    """

    def print_result(**kwargs):
        ti = kwargs['ti']
        query_result = ti.xcom_pull(task_ids='query_data_task')
        for row in query_result:
            print(row)

    create_table_task = PostgresOperator(
        task_id='create_table_task',
        postgres_conn_id='postgres_conn',
        sql=create_table_query,
        dag=dag
    )

    insert_data_task = PostgresOperator(
        task_id='insert_data_task',
        postgres_conn_id='postgres_conn',
        sql=insert_data,
        dag=dag
    )

    query_data_task = PostgresOperator(
        task_id='query_data_task',
        postgres_conn_id='postgres_conn',
        sql=query_data,
        dag=dag
    )

    print_result_task = PythonOperator(
        task_id='print_result_task',
        python_callable=print_result,
        provide_context=True,
        dag=dag
    )

    create_table_task >> insert_data_task >> query_data_task >> print_result_task
