from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional
import aiohttp

class BaseFetcher(ABC):
    @abstractmethod
    async def fetch(self, date: datetime, region: str) -> Optional[Path]:
        pass

    async def _download_file(self, url: str, output_path: Path) -> Optional[Path]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    with open(output_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            f.write(chunk)
                    return output_path
                return None
