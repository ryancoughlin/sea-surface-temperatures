from datetime import datetime
from pathlib import Path
from typing import Optional
from .base import DataFetcher
from ..config.settings import settings, FileDataset
from ..config.regions import RegionCode

class EastCoastFetcher(DataFetcher):
    def __init__(self):
        self.config = settings.SOURCES["east_coast_sst"]
    
    def _build_url(self, region: RegionCode, date: datetime) -> str:
        filename = self.config.file_pattern.format(
            date=date.strftime("%Y%j"),
            region=region.value.upper()
        )
        url = f"{self.config.base_url}/{filename}"
        print(f"EastCoast URL: {url}")
        return url

    async def fetch(self, dataset: FileDataset, region: RegionCode) -> Path:
        date = datetime.now()
        url = self._build_url(region, date)
        output_path = settings.RAW_PATH / "east_coast" / f"sst_{region.value}_{date.strftime('%Y%m%d')}.nc"
        return await self._download_file(url, output_path)