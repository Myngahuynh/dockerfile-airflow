import yfinance as yf
import pandas as pd
import os
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def crawl_and_save_stocks():
    """
    The main function to be executed by the PythonOperator.
    It crawls stock data and saves it to a CSV file.
    """
    # Define the output directory inside the Airflow container
    # The 'dags' folder is automatically mounted into /opt/airflow/dags
    output_dir = '/opt/airflow/dags/data'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # List of stock tickers to crawl
    tickers = ["AAPL", "GOOGL", "MSFT", "TSLA"]
    
    print("Starting stock data crawl...")
    for ticker in tickers:
        try:
            # Get stock data for the last day with a 1-minute interval
            stock = yf.Ticker(ticker)
            data = stock.history(period="1d", interval="1m")
            
            if data.empty:
                print(f"No data returned for {ticker}. Skipping.")
                continue

            # Define the CSV file path
            filename = os.path.join(output_dir, f"{ticker}_stock_data.csv")
            
            # Check if file exists to determine if we need to write the header
            file_exists = os.path.isfile(filename)
            
            # Append data to the CSV file
            data.to_csv(filename, mode='a', header=not file_exists)
            print(f"Data for {ticker} saved to {filename}")

        except Exception as e:
            print(f"Error crawling data for {ticker}: {e}")
    
    print("Stock data crawl finished.")


with DAG(
    dag_id='stock_crawling_pipeline',
    start_date=datetime(2025, 7, 15),
    schedule_interval='0 /2   ',  # This is a cron expression for "every 2 hours"
    catchup=False,
    tags=['stock', 'data-pipeline'],
    doc_md="""
    ### Stock Crawling Pipeline
    This DAG crawls stock data for specified tickers every 2 hours and saves it to a CSV file.
    """
) as dag:
    crawl_stock_task = PythonOperator(
        task_id='crawl_and_save_stocks',
        python_callable=crawl_and_save_stocks
    )