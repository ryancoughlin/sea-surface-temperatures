from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional, Union
from ..config.settings import ERDDAPDataset, FileDataset
from ..config.regions import RegionCode
import aiohttp
import ssl

class BaseFetcher(ABC):
    @abstractmethod
    async def fetch(self, date: datetime, region: str) -> Optional[Path]:
        """Fetch data with time lag applied."""
        adjusted_date = self._get_latest_available_time(date)
        region_code = RegionCode(region)
        url = self._build_url(region_code, adjusted_date)
        output_path = settings.RAW_PATH / "erddap" / f"sst_{region}_{adjusted_date.strftime('%Y%m%d')}.nc"
        return await self._download_file(url, output_path)
    
    async def _download_file(self, url: str, output_path: Path) -> Path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create SSL context that skips verification
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, ssl=ssl_context) as response:
                if response.status != 200:
                    raise ValueError(f"Failed to fetch data: {response.status}")
                
                with open(output_path, 'wb') as f:
                    while True:
                        chunk = await response.content.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)
        
        return output_path
