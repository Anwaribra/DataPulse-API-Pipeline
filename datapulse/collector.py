import os
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any
import aiohttp
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class CoinGeckoCollector:
    def __init__(self):
        self.api_url = os.getenv("COINGECKO_API_URL")
        self.api_key = os.getenv("COINGECKO_API_KEY")
        self.supported_coins = os.getenv("SUPPORTED_COINS", "").split(",")
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={"X-CG-API-KEY": self.api_key} if self.api_key else {}
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_price_data(self, coin_id: str) -> Dict[str, Any]:
        """Fetch current price data for a specific coin."""
        try:
            url = f"{self.api_url}/simple/price"
            params = {
                "ids": coin_id,
                "vs_currencies": "usd",
                "include_24hr_change": "true",
                "include_market_cap": "true",
                "include_total_volume": "true"
            }
            
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                
                if coin_id in data:
                    return {
                        "coin_id": coin_id,
                        "price_usd": data[coin_id]["usd"],
                        "price_change_24h": data[coin_id]["usd_24h_change"],
                        "market_cap": data[coin_id]["usd_market_cap"],
                        "total_volume": data[coin_id]["usd_24h_vol"],
                        "timestamp": datetime.utcnow().isoformat()
                    }
                return None
        except Exception as e:
            logger.error(f"Error fetching data for {coin_id}: {str(e)}")
            return None

    async def collect_all_prices(self) -> List[Dict[str, Any]]:
        """Collect price data for all supported coins."""
        tasks = [self.fetch_price_data(coin) for coin in self.supported_coins]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

async def run_collector():
    """Main collector loop."""
    while True:
        try:
            async with CoinGeckoCollector() as collector:
                data = await collector.collect_all_prices()
                # TODO: Store data in database
                logger.info(f"Collected data for {len(data)} coins")
        except Exception as e:
            logger.error(f"Error in collector loop: {str(e)}")
        
        await asyncio.sleep(int(os.getenv("COLLECTION_INTERVAL", 60))) 