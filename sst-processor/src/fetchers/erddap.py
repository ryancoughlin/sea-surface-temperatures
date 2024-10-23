from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from .base import BaseFetcher
from ..config.settings import settings
from ..config.regions import REGIONS, RegionCode

class ERDDAPFetcher(BaseFetcher):
    def __init__(self):
        self.config = settings.SOURCES["erddap"]
    
    def _get_latest_available_time(self, current_time: datetime) -> datetime:
        """Get latest available time accounting for update lag."""
        lag = timedelta(hours=self.config.time_lag_hours)
        return current_time - lag

    def _build_url(self, region: str, date: datetime) -> str:
        region_code = RegionCode(region)
        region_bounds = REGIONS[region_code].bounds
        time_str = date.strftime(self.config.time_format)
        
        query = (
            f"{self.config.dataset_id}.nc?"
            f"{','.join(self.config.variables)}"
            f"[({time_str}):1:({time_str})]"
            f"[({region_bounds.lat[0]}):1:({region_bounds.lat[1]})]"
            f"[({region_bounds.lon[0]}):1:({region_bounds.lon[1]})]"
        )
        url = f"{self.config.base_url}/{query}"
        print(f"ERDDAP URL: {url}")
        return url

    async def fetch(self, date: datetime, region: str) -> Optional[Path]:
        """Fetch data with time lag applied."""
        adjusted_date = self._get_latest_available_time(date)
        url = self._build_url(region, adjusted_date)
        output_path = settings.RAW_PATH / "erddap" / f"sst_{region}_{date.strftime('%Y%m%d')}.nc"
        return await self._download_file(url, output_path)
