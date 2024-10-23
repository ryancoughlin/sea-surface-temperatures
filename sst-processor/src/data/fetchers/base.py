from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional
import aiohttp
import ssl

class BaseFetcher(ABC):
    @abstractmethod
    async def fetch(self, date: datetime, region: str) -> Optional[Path]:
        pass

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
