from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
import pendulum

with DAG(
    'my-first-airflow-docker',
    description='A tutorial for your first Airflow DAG running on Docker',
    start_date=pendulum.today('UTC').add(days=-1),
    # schedule_interval='@daily',
    schedule_interval='*/5 * * * *',  # CRON
    catchup=False
    ) as dag:

    def hello_world(data_interval_end):
        print(f"Hello, World! With Airflow on Docker on day {data_interval_end} till 7 days more on {ds_add(data_interval_end, 7)}!")

    tarefa_1 = BashOperator(
        task_id = 'create_dir',
        bash_command = 'mkdir -p "/opt/airflow/test/test_dir"'
    )
    tarefa_2 = BashOperator(
        task_id = 'create_file',
        bash_command = 'touch "/opt/airflow/test/test_dir/test.txt={{ data_interval_end.strftime("%Y-%m-%d") }}"'
    )
    tarefa_3 = BashOperator(
        task_id = 'write_file',
        bash_command = 'echo "Hello, World!" > "/opt/airflow/test/test_dir/test.txt"'
    )
    tarefa_4 = PythonOperator(
        task_id = 'hello_world',
        python_callable = hello_world,
        op_kwargs = {'data_interval_end': '{{ data_interval_end.strftime("%Y-%m-%d") }}'}
    )
        
    tarefa_1 >> tarefa_2 >> tarefa_3 >> tarefa_4
