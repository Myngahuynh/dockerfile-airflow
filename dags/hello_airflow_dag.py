# Filename: dags/hello_airflow_dag.py
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='hello_airflow_dag',
    start_date=datetime(2025, 6, 28),
    schedule_interval=None,  # This DAG will only run when you trigger it manually
    catchup=False,
    tags=['example', 'starter'],
) as dag:

    # Task 1: Print the current date
    print_date_task = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # Task 2: Print a hello message
    say_hello_task = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello Airflow! Your basic setup is working."',
    )

    # Task 3: Print a hello message
    say_bye_task = BashOperator(
        task_id='say_goodbye',
        bash_command='echo "Bye, it is easy to learn."',
    )

    # Define the task dependency
    print_date_task >> say_hello_task >> say_bye_task