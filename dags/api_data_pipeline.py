from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import logging
import time

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 26),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "api_data_pipeline",
    default_args=default_args,
    schedule="0 */3 * * *",
    catchup=False,
)


def fetch_data():
    """Fetches data from CoinGecko API."""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "bitcoin,ethereum", "vs_currencies": "usd"}
    
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            logging.info(f"Fetched Data: {data}")
            save_to_db(data)
            return
        except requests.RequestException as e:
            wait_time = (2 ** attempt) * 30
            logging.warning(f"Attempt {attempt + 1} failed: {e}. Waiting {wait_time}s")
            if attempt < 2:
                time.sleep(wait_time)
    
    logging.error("All retry attempts failed")
    raise Exception("Failed to fetch data after all retries")

def save_to_db(data):
    """Saves fetched data to PostgreSQL."""
    try:
        conn = psycopg2.connect(
            database="airflow",
            user="PostgreSQL",
            password="2003",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        
        for coin, values in data.items():
            cursor.execute(
                "INSERT INTO crypto_prices (coin, price_usd, timestamp) VALUES (%s, %s, NOW())",
                (coin, values["usd"])
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Data successfully saved to database.")
    
    except psycopg2.DatabaseError as e:
        logging.error(f"Database Error: {e}")

# Airflow task
fetch_task = PythonOperator(
    task_id="fetch_api_data",
    python_callable=fetch_data,
    dag=dag,
)

fetch_task 
