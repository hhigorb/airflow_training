from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

"""
catchup: Ao inserir o parâmetro catchup como True, o Airflow irá
rodar todo o período passado.
Se o start_date for uma data muito antiga e a DAG foi iniciada
hoje, catchup fará com que todo o período seja executado.
Se True, todas as tarefas que não foram executadas no período definido
serão executadas imediatamente quando a DAG é iniciada.

depends_on_past: indica se as tarefas devem ser executadas em relação
à sua execução anterior. Se definido como True, a execução da tarefa
atual será condicionada à execução bem-sucedida da tarefa anterior.
Se definido como False (como no exemplo), cada tarefa será executada
independentemente do resultado da tarefa anterior.

retries: define o número máximo de vezes que uma tarefa pode ser
reexecutada em caso de falha.

retry_delay: define o intervalo de tempo mínimo entre as tentativas de
execução de uma tarefa em caso de falha.

start_date: Data de início da DAG. Todas as tarefas anteriores à data de
início serão marcadas como "concluídas".

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

with DAG(dag_id='primeira_dag',
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

    task1 >> task2 >> task3
