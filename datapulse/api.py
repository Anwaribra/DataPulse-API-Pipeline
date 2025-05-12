from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
import pandas as pd
from .models import PriceData, get_db
from .collector import CoinGeckoCollector
import asyncio
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="DataPulse API", version="1.0.0")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
 
    asyncio.create_task(CoinGeckoCollector().run_collector())

@app.get("/api/v1/prices/latest")
async def get_latest_prices(
    coin_id: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get the latest price data for all coins or a specific coin."""
    query = db.query(PriceData)
    if coin_id:
        query = query.filter(PriceData.coin_id == coin_id)
    
    latest_data = query.order_by(PriceData.timestamp.desc()).all()
    return [data.to_dict() for data in latest_data]

@app.get("/api/v1/prices/history")
async def get_price_history(
    coin_id: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    interval: str = Query("1h", regex="^(1m|5m|15m|1h|4h|1d)$"),
    db: Session = Depends(get_db)
):
    """Get historical price data with optional aggregation."""
    query = db.query(PriceData).filter(PriceData.coin_id == coin_id)
    
    if start_time:
        query = query.filter(PriceData.timestamp >= start_time)
    if end_time:
        query = query.filter(PriceData.timestamp <= end_time)
    
    data = query.order_by(PriceData.timestamp).all()
    
    if not data:
        raise HTTPException(status_code=404, detail="No data found for the specified parameters")
    

    df = pd.DataFrame([d.to_dict() for d in data])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    
    
    resampled = df.resample(interval).agg({
        'price_usd': 'mean',
        'price_change_24h': 'mean',
        'market_cap': 'mean',
        'total_volume': 'sum'
    }).fillna(method='ffill')
    
    return resampled.reset_index().to_dict(orient='records')

@app.get("/api/v1/coins")
async def get_supported_coins():
    """Get list of supported cryptocurrencies."""
    return {"coins": CoinGeckoCollector().supported_coins}

@app.get("/api/v1/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()} 