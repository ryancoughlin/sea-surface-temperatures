import xarray as xr
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from ..config.settings import settings
from ..fetchers.erddap import ERDDAPFetcher
from ..image_generator import ImageGenerator
from .image_processor import ImageProcessor
from ..config.regions import REGIONS, RegionCode

class SSTProcessor:
    """Handles SST data processing and image generation from ERDDAP source."""
    
    def __init__(self):
        self.image_generator = ImageGenerator()
        self.image_processor = ImageProcessor()
        self.fetcher = ERDDAPFetcher()

    async def process_latest(self) -> Dict[str, Dict[str, Dict[int, List[Path]]]]:
        """Process latest SST data from ERDDAP source."""
        date = datetime.now()
        results = {}
        
        for region in REGIONS:
            try:
                paths = await self.process_region(date, region)
                results[f"erddap_{region.value}"] = paths
            except Exception as e:
                print(f"Error processing ERDDAP {region.value}: {e}")
        
        return results

    async def process_region(self, date: datetime, region: RegionCode) -> Dict[str, Dict[int, List[Path]]]:
        """Process SST data for a specific region."""
        input_file = await self.fetch_data(date, region)
        if not input_file:
            raise ValueError(f"Failed to fetch data for {region.value} on {date}")
        
        sst, lat, lon = self.load_sst_data(input_file)
        return await self.image_generator.generate_images(sst, lat, lon, "erddap", region.value, date)

    async def fetch_data(self, date: datetime, region: RegionCode) -> Optional[Path]:
        return await self.fetcher.fetch(settings.SOURCES["blended_sst"], region)

    def load_sst_data(self, input_file: Path) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Load SST data from a file."""
        return self.image_processor.load_and_process(input_file, "erddap")

    def _smooth_sst(self, sst: np.ndarray) -> np.ndarray:
        """Smooth SST data."""
        # Implement smoothing SST data
        pass

    def _increase_resolution(self, sst: np.ndarray, scale: int) -> np.ndarray:
        """Increase resolution of SST data."""
        # Implement increasing resolution of SST data
        pass
