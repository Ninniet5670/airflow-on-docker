from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import pendulum

with DAG(
    'my-first-airflow-docker',
    description='A tutorial for your first Airflow DAG running on Docker',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule_interval='@daily'
    ) as dag:

    def hello_world():
        print("Hello, World! With Airflow on Docker!")

    tarefa_1 = BashOperator(
        task_id = 'create_dir',
        bash_command = 'mkdir -p "/opt/airflow/test/test_dir"'
    )
    tarefa_2 = BashOperator(
        task_id = 'create_file',
        bash_command = 'touch "/opt/airflow/test/test_dir/test.txt"'
    )
    tarefa_3 = BashOperator(
        task_id = 'write_file',
        bash_command = 'echo "Hello, World!" > "/opt/airflow/test/test_dir/test.txt"'
    )
    tarefa_4 = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world
    )
        
    tarefa_1 >> tarefa_2 >> tarefa_3 >> tarefa_4
