from datetime import datetime
from pathlib import Path
from typing import Optional
from .base import BaseFetcher
from ...config.settings import settings
from ...config.regions import REGIONS, RegionCode

class ERDDAPFetcher(BaseFetcher):
    def __init__(self):
        self.config = settings.SOURCES["erddap"]
    
    def _build_url(self, region: str, date: datetime) -> str:
        region_code = RegionCode(region)
        region_bounds = REGIONS[region_code].bounds
        time_str = date.strftime(self.config.time_format)
        
        query = (
            f"{self.config.dataset_id}.nc?"
            f"{','.join(self.config.variables)}"
            f"&time={time_str}"
            f"&latitude>={region_bounds.lat[0]}&latitude<={region_bounds.lat[1]}"
            f"&longitude>={region_bounds.lon[0]}&longitude<={region_bounds.lon[1]}"
        )
        return f"{self.config.base_url}/{query}"

    async def fetch(self, date: datetime, region: str) -> Optional[Path]:
        url = self._build_url(region, date)
        output_path = settings.RAW_PATH / "erddap" / f"sst_{region}_{date.strftime('%Y%m%d')}.nc"
        return await self._download_file(url, output_path)
