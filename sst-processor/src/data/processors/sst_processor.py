import xarray as xr
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from scipy.signal import savgol_filter
from scipy.interpolate import RegularGridInterpolator
from ..fetchers.erddap import ERDDAPFetcher
from ..fetchers.eastcoast import EastCoastFetcher
from ...tiles.generator import TileGenerator
from ...config.settings import settings
from .image_processor import ImageProcessor

class SSTProcessor:
    """Handles SST data processing and image generation from multiple sources."""
    
    def __init__(self):
        self.tile_generator = TileGenerator()
        self.image_processor = ImageProcessor()
        self.erddap_fetcher = ERDDAPFetcher()
        self.eastcoast_fetcher = EastCoastFetcher()

    async def process_latest(self) -> Dict[str, List[Path]]:
        """Process latest SST data from all configured sources."""
        date = datetime.now()
        results = {}
        
        for source, config in settings.SOURCES.items():
            for region in config.regions:
                try:
                    paths = await self.process_region(date, region, source)
                    results[f"{source}_{region}"] = paths
                except Exception as e:
                    print(f"Error processing {source} {region}: {e}")
        
        return results

    async def process_region(self, date: datetime, region: str, source: str) -> List[Path]:
        """Process SST data for a specific region."""
        input_file = await self.fetch_data(date, region, source)
        if not input_file:
            raise ValueError(f"Failed to fetch data for {region} on {date}")
        
        sst, lat, lon = self.load_sst_data(input_file)
        return self.generate_all_zoom_levels(sst, lat, lon)

    def generate_all_zoom_levels(self, sst: np.ndarray, lat: np.ndarray, lon: np.ndarray) -> List[Path]:
        """Generate tiles for all zoom levels."""
        tile_paths = []
        for zoom in settings.ZOOM_LEVELS:
            processed_sst = self._process_for_zoom(sst, zoom)
            paths = self.tile_generator.generate_tiles(processed_sst, lat, lon, zoom, settings.TILE_PATH)
            tile_paths.extend(paths)
        return tile_paths

    def _process_for_zoom(self, sst: np.ndarray, zoom: int) -> np.ndarray:
        """Process SST data for a specific zoom level."""
        return self.image_processor.process_for_zoom(sst, zoom)

    async def fetch_data(self, date: datetime, region: str, source: str = "erddap") -> Optional[Path]:
        if source == "erddap":
            return await self.erddap_fetcher.fetch(date, region)
        elif source == "east_coast":
            return await self.eastcoast_fetcher.fetch(date, region)
        raise ValueError(f"Unknown source: {source}")

    def load_sst_data(self, input_file: Path) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Load SST data from a file."""
        return self.image_processor.load_and_process(input_file)

    def _smooth_sst(self, sst: np.ndarray) -> np.ndarray:
        """Smooth SST data."""
        # Implement smoothing SST data
        pass

    def _increase_resolution(self, sst: np.ndarray, scale: int) -> np.ndarray:
        """Increase resolution of SST data."""
        # Implement increasing resolution of SST data
        pass

