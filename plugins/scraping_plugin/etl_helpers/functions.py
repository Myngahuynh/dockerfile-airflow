# Filename: plugins/etl_helpers/functions.py
"""
This module contains helper functions for the user ETL pipeline.
"""
from __future__ import annotations
import requests
import pandas as pd
import logging
import os

# Set up a logger
log = logging.getLogger(__name__)

def _extract_users_data(url: str) -> list[dict]:
    """Extracts user data from a given API endpoint."""
    log.info(f"Extracting user data from {url}...")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        users = response.json()
        log.info(f"Successfully extracted {len(users)} users.")
        return users
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to extract data: {e}")
        raise

def _transform_users_data(users: list[dict]) -> pd.DataFrame:
    """Transforms the raw user data into a clean pandas DataFrame."""
    log.info(f"Transforming {len(users)} users...")
    if not users:
        log.warning("User list is empty. Returning an empty DataFrame.")
        return pd.DataFrame()
        
    transformed_users = []
    for user in users:
        transformed_users.append({
            'user_id': user.get('id'),
            'name': user.get('name'),
            'username': user.get('username'),
            'email': user.get('email'),
            'company_name': user.get('company', {}).get('name'),
            'city': user.get('address', {}).get('city')
        })
    
    df = pd.DataFrame(transformed_users)
    log.info("Transformation complete.")
    return df

def _load_data_to_csv(df: pd.DataFrame, output_path: str):
    """Saves a pandas DataFrame to a CSV file."""
    log.info(f"Loading DataFrame to CSV at {output_path}...")
    if df.empty:
        log.warning("DataFrame is empty, skipping CSV creation.")
        return
        
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        log.info(f"Successfully saved DataFrame with {len(df)} rows to {output_path}.")
    except Exception as e:
        log.error(f"Failed to save DataFrame to CSV: {e}")
        raise

def run_full_etl_process(api_url: str, output_path: str):
    """
    Runs the full ETL process: Extract, Transform, and Load.
    
    Args:
        api_url: The URL to extract data from.
        output_path: The path to save the final CSV file.
    """
    log.info("Starting full ETL process...")
    # Step 1: Extract
    raw_data = _extract_users_data(url=api_url)
    
    # Step 2: Transform
    transformed_data = _transform_users_data(users=raw_data)
    
    # Step 3: Load
    _load_data_to_csv(df=transformed_data, output_path=output_path)
    log.info("Full ETL process completed successfully.")

def aggregate_user_data(input_path: str, output_path: str):
    """
    Reads user data from a CSV, aggregates it by city, and saves the result.
    """
    log.info(f"Reading data from {input_path} for aggregation...")
    try:
        df = pd.read_csv(input_path)
        log.info("Calculating user count by city...")
        city_counts = df.groupby('city').size().reset_index(name='user_count')
        
        log.info(f"Saving aggregated data to {output_path}...")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        city_counts.to_csv(output_path, index=False)
        log.info("Aggregation and save complete.")
    except FileNotFoundError:
        log.error(f"The file was not found at the specified path: {input_path}")
        raise
    except Exception as e:
        log.error(f"An error occurred during aggregation: {e}")
        raise