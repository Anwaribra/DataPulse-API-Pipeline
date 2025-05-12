from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()

class PriceData(Base):
    __tablename__ = "price_data"

    id = Column(Integer, primary_key=True)
    coin_id = Column(String, index=True)
    price_usd = Column(Float)
    price_change_24h = Column(Float)
    market_cap = Column(Float)
    total_volume = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "coin_id": self.coin_id,
            "price_usd": self.price_usd,
            "price_change_24h": self.price_change_24h,
            "market_cap": self.market_cap,
            "total_volume": self.total_volume,
            "timestamp": self.timestamp.isoformat()
        }

# DB connection 
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 