import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

"""
Diferente de providers, hooks são componentes de mais baixo nível.
Com eles, é possível instanciar o objeto e utilizar os métodos ao invés
de apenas criar tasks pré-definidas como os providers
"""

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


with DAG(dag_id='hooks',
         default_args=default_args,
         description='airflow_test_dag',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=4, day=30),
         schedule_interval='@daily',
         catchup=False,
         tags=['airflow_dag']) as dag:

    def create_table():
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        pg_hook.run(
            'CREATE TABLE IF NOT EXISTS public.teste (id int);', autocommit=True)

    def insert_data():
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        pg_hook.run('INSERT INTO public.teste (id) VALUES (1);',
                    autocommit=True)

    def query_data(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        # get_records quando quero retornar registros.
        # o run quando quero executar uma query que não retorna registros.
        data = pg_hook.get_records(
            'SELECT * FROM public.teste;')
        kwargs['ti'].xcom_push(key='query_result', value=data)

    def print_result(**kwargs):
        task_instance = kwargs['ti'].xcom_pull(
            key='query_result', task_ids='query_data_task')

        print('Dados da tabela:')
        for row in task_instance:
            print(row)

    create_table_task = PythonOperator(
        task_id='create_table_task',
        python_callable=create_table,
        dag=dag
    )

    insert_data_task = PythonOperator(
        task_id='insert_data_task',
        python_callable=insert_data,
        dag=dag
    )

    query_data_task = PythonOperator(
        task_id='query_data_task',
        python_callable=query_data,
        provide_context=True,
        dag=dag
    )

    print_result_task = PythonOperator(
        task_id='print_result_task',
        python_callable=print_result,
        provide_context=True,
        dag=dag
    )

    create_table_task >> insert_data_task >> query_data_task >> print_result_task
