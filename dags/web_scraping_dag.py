# Filename: dags/web_scraping_dag.py
from airflow.models.dag import DAG
from scraping_plugin.operators.scraping_operator import WebScrapingOperator
from datetime import datetime

with DAG(
    dag_id='web_scraping_dag',
    start_date=datetime(2025, 6, 28),
    schedule_interval=None,
    catchup=False,
    tags=['example', 'web-scraping'],
) as dag:

    scrape_books_task = WebScrapingOperator(
        task_id='scrape_books',
        url='http://books.toscrape.com/',
        output_path='/opt/airflow/dags/scraped_books.csv',
    )