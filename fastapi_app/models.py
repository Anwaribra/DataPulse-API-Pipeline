from pydantic import BaseModel
from datetime import datetime

class CryptoPrice(BaseModel):
    coin: str
    price_usd: float
    timestamp: datetime

    class Config:
        from_attributes = True  # Allows ORM model conversion
