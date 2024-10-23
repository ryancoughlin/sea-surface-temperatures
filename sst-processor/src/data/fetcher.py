import aiohttp
import asyncio
from pathlib import Path
import xarray as xr
from datetime import datetime

from ..config.settings import settings

async def fetch_sst_data(date: str) -> Path:
    """
    Fetches SST data from NOAA for a given date.
    
    Args:
        date: Date string in YYYY-MM-DD format
        
    Returns:
        Path to downloaded file
    """
    url = f"{settings.NOAA_BASE_URL}/SST_{date}.nc"
    file_path = settings.RAW_PATH / f"sst_{date}.nc4"
    
    settings.RAW_PATH.mkdir(parents=True, exist_ok=True)
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                raise ValueError(f"Failed to fetch data: {response.status}")
            
            with open(file_path, 'wb') as f:
                while True:
                    chunk = await response.content.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
    
    return file_path
