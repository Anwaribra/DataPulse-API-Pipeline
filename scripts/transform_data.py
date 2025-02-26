import pandas as pd
from datetime import datetime
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Establish database connection"""
    return psycopg2.connect(
        database="airflow",
        user="postgres",
        password="2003",
        host="localhost",
        port="5432"
    )

def fetch_raw_data():
    """Fetch raw data from database"""
    try:
        conn = get_db_connection()
        query = "SELECT coin, price_usd, timestamp FROM crypto_prices"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return None

def transform_data(df):
    """Transform the cryptocurrency price data"""
    if df is None or df.empty:
        return None
    
    try:
        # Convert timestamp to datetime if not already
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Calculate daily average prices
        daily_avg = df.groupby(['coin', df['timestamp'].dt.date])['price_usd'].agg([
            'mean',
            'min',
            'max',
            'std'
        ]).reset_index()
        
        # Rename columns
        daily_avg.columns = ['coin', 'date', 'avg_price', 'min_price', 'max_price', 'price_std']
        
        # Round numerical columns to 2 decimal places
        numeric_columns = ['avg_price', 'min_price', 'max_price', 'price_std']
        daily_avg[numeric_columns] = daily_avg[numeric_columns].round(2)
        
        return daily_avg
    
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        return None

def save_transformed_data(df):
    """Save transformed data back to database"""
    if df is None or df.empty:
        return False
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS crypto_prices_daily (
            coin VARCHAR(50),
            date DATE,
            avg_price NUMERIC,
            min_price NUMERIC,
            max_price NUMERIC,
            price_std NUMERIC,
            PRIMARY KEY (coin, date)
        );
        """
        cursor.execute(create_table_query)
        
        # Insert data
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO crypto_prices_daily (coin, date, avg_price, min_price, max_price, price_std)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (coin, date) 
            DO UPDATE SET 
                avg_price = EXCLUDED.avg_price,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                price_std = EXCLUDED.price_std;
            """
            cursor.execute(insert_query, (
                row['coin'],
                row['date'],
                row['avg_price'],
                row['min_price'],
                row['max_price'],
                row['price_std']
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Successfully saved transformed data")
        return True
    
    except Exception as e:
        logger.error(f"Error saving transformed data: {e}")
        return False

def main():
    """Main function to run the transformation pipeline"""
    raw_data = fetch_raw_data()
    if raw_data is not None:
        transformed_data = transform_data(raw_data)
        if transformed_data is not None:
            save_transformed_data(transformed_data)

if __name__ == "__main__":
    main()
