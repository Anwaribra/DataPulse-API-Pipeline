from fastapi import FastAPI
import psycopg2

app = FastAPI()

def get_data():
    conn = psycopg2.connect(database="airflow", user="PostgreSQL", password="2003", host="localhost", port="5432")
    cursor = conn.cursor()
    cursor.execute("SELECT coin, price_usd, timestamp FROM crypto_prices ORDER BY timestamp DESC LIMIT 10")
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"coin": row[0], "price_usd": row[1], "timestamp": row[2]} for row in data]

@app.get("/crypto-prices")
def crypto_prices():
    return get_data()
