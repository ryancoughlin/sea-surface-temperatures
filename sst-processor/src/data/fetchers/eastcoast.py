from datetime import datetime
from pathlib import Path
from typing import Optional
from .base import BaseFetcher
from ...config.settings import settings

class EastCoastFetcher(BaseFetcher):
    def __init__(self):
        self.config = settings.SOURCES["east_coast"].avhrr_viirs
    
    def _build_url(self, region: str, date: datetime) -> str:
        filename = self.config.file_format.format(
            date=date.strftime("%Y%j"),
            time_range="DAILY",
            region=region.upper()
        )
        url = f"{self.config.base_url}/{filename}"
        print(f"EastCoast URL: {url}")
        return url

    async def fetch(self, date: datetime, region: str) -> Optional[Path]:
        url = self._build_url(region, date)
        output_path = settings.RAW_PATH / "east_coast" / f"sst_{region}_{date.strftime('%Y%m%d')}.nc"
        return await self._download_file(url, output_path)
