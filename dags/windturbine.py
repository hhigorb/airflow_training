import json
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

doc_md = """
## Documentação da DAG
"""

dag_owner = 'data-engineer-team'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'email': 'hhigorb@outlook.com',
                'email_on_failure': True,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5)
                }

with DAG(dag_id='windturbine',
         default_args=default_args,
         description='airflow_test_dag',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=4, day=30),
         schedule_interval='@daily',
         catchup=False,
         tags=['airflow_dag']) as dag:

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS windturbine (
        idtemp VARCHAR,
        powerfactor VARCHAR,
        hydraulicpressure VARCHAR,
        temperature VARCHAR,
        timestamp VARCHAR
    );
    """

    insert_into_sql = """
    INSERT INTO windturbine (idtemp, powerfactor, hydraulicpressure, temperature, timestamp)
    VALUES (%s, %s, %s, %s, %s);"""

    def process_file(**kwargs):
        with open(Variable.get('file_path')) as json_file:
            data = json.load(json_file)

            # O parâmetro key serve para identificar o valor que eu quero pegar do json
            # Já o value serve para identificar o valor que eu quero passar para o xcom
            kwargs['ti'].xcom_push(key='idtemp', value=data['idtemp'])
            kwargs['ti'].xcom_push(
                key='powerfactor', value=data['powerfactor'])
            kwargs['ti'].xcom_push(
                key='hydraulicpressure', value=data['hydraulicpressure'])
            kwargs['ti'].xcom_push(
                key='temperature', value=data['temperature'])
            kwargs['ti'].xcom_push(key='timestamp', value=data['timestamp'])

        os.remove(Variable.get('file_path'))

    def check_temp(**kwargs):
        temperature = float(kwargs['ti'].xcom_pull(
            task_ids='get_data_task', key='temperature'))

        if temperature > 24:
            return 'check_temp_group.send_email_alert_task'
        else:
            return 'check_temp_group.send_email_task'

    check_temp_group = TaskGroup(
        group_id='check_temp_group',
        dag=dag)

    database_group = TaskGroup(
        group_id='database',
        dag=dag)

    file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        filepath=Variable.get('file_path'),
        fs_conn_id='fs_default',
        poke_interval=10,
        dag=dag
    )

    get_data_task = PythonOperator(
        task_id='get_data_task',
        python_callable=process_file,
        provide_context=True,
        dag=dag
    )

    create_table_task = PostgresOperator(
        task_id='create_table_task',
        postgres_conn_id='postgres_conn',
        sql=create_table_sql,
        task_group=database_group,
        dag=dag
    )

    insert_data_task = PostgresOperator(
        task_id='insert_data_task',
        postgres_conn_id='postgres_conn',
        parameters=(
            '{{ ti.xcom_pull(task_ids="get_data_task", key="idtemp") }}',
            '{{ ti.xcom_pull(task_ids="get_data_task", key="powerfactor") }}',
            '{{ ti.xcom_pull(task_ids="get_data_task", key="hydraulicpressure") }}',
            '{{ ti.xcom_pull(task_ids="get_data_task", key="temperature") }}',
            '{{ ti.xcom_pull(task_ids="get_data_task", key="timestamp") }}'
        ),
        sql=insert_into_sql,
        task_group=database_group,
        dag=dag

    )

    send_email_alert_task = EmailOperator(
        task_id='send_email_alert_task',
        to='hhigorb@outlook.com',
        subject='Airflow Alert',
        html_content=""" <h3>Alerta de temperatura</h3> """,
        task_group=check_temp_group,
        dag=dag
    )

    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='hhigorb@outlook.com',
        subject='Airflow Alert',
        html_content=""" <h3>Temperaturas estão estáveis</h3> """,
        task_group=check_temp_group,
        dag=dag
    )

    check_temp_task = BranchPythonOperator(
        task_id='check_temp_task',
        python_callable=check_temp,
        provide_context=True,
        task_group=check_temp_group,
        dag=dag
    )

    with check_temp_group:
        check_temp_task >> [send_email_alert_task, send_email_task]

    with database_group:
        create_table_task >> insert_data_task

    file_sensor_task >> get_data_task
    get_data_task >> check_temp_group
    get_data_task >> database_group
