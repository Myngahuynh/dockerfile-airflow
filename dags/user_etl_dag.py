# Filename: dags/user_etl_dag.py
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

# Import the helper functions from our custom plugin
from scraping_plugin.etl_helpers import functions as etl

# --- Unified ETL Pipeline DAG ---
@dag(
    dag_id="user_full_etl_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2025, 6, 28, tz="UTC"),
    catchup=False,
    doc_md="""
    ## User Full ETL Pipeline
    This DAG orchestrates the entire user data ETL process in a single, ordered workflow.
    It extracts data from an API, transforms it, and saves it to a CSV file.
    A second step then aggregates this data.
    All business logic is encapsulated in the `etl_helpers` plugin.
    """,
    tags=["etl", "unified-pipel ine"],
)
def user_full_etl_pipeline():
    """
    ### Full ETL Pipeline
    A single pipeline that runs the extract, transform, load, and aggregate steps in order.
    """
    
    @task
    def run_etl():
        """
        #### Run ETL Task
        This single task executes the entire ETL process by calling a helper function.
        """
        api_url = "https://jsonplaceholder.typicode.com/users"
        output_path = "/opt/airflow/dags/data/final_users.csv"
        etl.run_full_etl_process(api_url=api_url, output_path=output_path)

    @task
    def aggregate_data():
        """
        #### Aggregate Data Task
        This task takes the processed user data and aggregates it.
        """
        input_path = "/opt/airflow/dags/data/final_users.csv"
        output_path = "/opt/airflow/dags/data/aggregated_user_data.csv"
        etl.aggregate_user_data(input_path=input_path, output_path=output_path)

    # Define the task dependency
    run_etl() >> aggregate_data()

# Instantiate the DAG
user_full_etl_pipeline()