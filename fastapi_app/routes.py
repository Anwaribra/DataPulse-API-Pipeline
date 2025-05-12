from fastapi import APIRouter, HTTPException
from typing import List
import psycopg2
from .models import CryptoPrice

router = APIRouter()

def get_db_connection():
    return psycopg2.connect(
        database="airflow",
        user="postgres",
        password="2003",
        host="localhost",
        port="5432"
    )

@router.get("/crypto-prices", response_model=List[CryptoPrice])
async def get_crypto_prices():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT coin, price_usd, timestamp FROM crypto_prices ORDER BY timestamp DESC LIMIT 10")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return [
            CryptoPrice(coin=row[0], price_usd=row[1], timestamp=row[2])
            for row in data
        ]
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=str(e))
