import xarray as xr
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from ..tiles.generator import TileGenerator
from .fetchers.erddap import ERDDAPFetcher
from .fetchers.eastcoast import EastCoastFetcher
from ..config.settings import settings

class SSTProcessor:
    """Handles all SST data processing and image generation."""
    
    def __init__(self):
        self.tile_generator = TileGenerator()
        self.erddap_fetcher = ERDDAPFetcher()
        self.eastcoast_fetcher = EastCoastFetcher()
        
    def load_sst_data(self, nc4_filepath: Path) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Load and convert SST data from NC4 file."""
        with xr.open_dataset(nc4_filepath) as ds:
            sst = ds.sst.squeeze().values
            lat = ds.lat.values
            lon = ds.lon.values
        
        sst_fahrenheit = (sst * 9/5) + 32
        print(f"Shape: {sst_fahrenheit.shape}")
        print(f"Min: {np.nanmin(sst_fahrenheit):.2f}, Max: {np.nanmax(sst_fahrenheit):.2f}")
        
        return sst_fahrenheit, lat, lon

    def smooth_sst(self, sst: np.ndarray, window_length: int = 11, polyorder: int = 2) -> np.ndarray:
        """Apply Savitzky-Golay filter for smoothing."""
        smoothed = savgol_filter(sst, window_length=window_length, polyorder=polyorder, axis=0, mode='nearest')
        smoothed = savgol_filter(smoothed, window_length=window_length, polyorder=polyorder, axis=1, mode='nearest')
        return smoothed

    def increase_resolution(self, sst: np.ndarray, scale_factor: int) -> np.ndarray:
        """Increase resolution with smoothing and interpolation."""
        original_grid = (np.arange(sst.shape[0]), np.arange(sst.shape[1]))
        interpolator = RegularGridInterpolator(original_grid, sst, bounds_error=False, fill_value=np.nan)
        
        new_y = np.linspace(0, sst.shape[0] - 1, sst.shape[0] * scale_factor)
        new_x = np.linspace(0, sst.shape[1] - 1, sst.shape[1] * scale_factor)
        new_grid = np.meshgrid(new_y, new_x, indexing='ij')
        
        return interpolator((new_grid[0], new_grid[1]))

    async def fetch_data(self, date: datetime, region: str, source: str = "erddap") -> Optional[Path]:
        """Fetch SST data from specified source."""
        if source == "erddap":
            return await self.erddap_fetcher.fetch(date, region)
        elif source == "east_coast":
            return await self.eastcoast_fetcher.fetch(date, region)
        raise ValueError(f"Unknown source: {source}")

    async def process_region(self, date: datetime, region: str, source: str) -> List[Path]:
        """Process SST data for a specific region."""
        input_file = await self.fetch_data(date, region, source)
        if not input_file:
            raise ValueError(f"Failed to fetch data for {region} on {date}")
        return await self.process_file(input_file, settings.TILE_PATH)

    async def process_file(self, input_file: Path, output_dir: Path) -> List[Path]:
        """Process a single SST file."""
        sst, lat, lon = self.load_sst_data(input_file)
        
        tile_paths = []
        for zoom in settings.ZOOM_LEVELS:
            processed_sst = self._process_zoom_level(sst, zoom)
            paths = self.tile_generator.generate_tiles(
                processed_sst, lat, lon, zoom, output_dir
            )
            tile_paths.extend(paths)
        
        return tile_paths

    def _process_zoom_level(self, sst: np.ndarray, zoom: int) -> np.ndarray:
        """Process SST data for specific zoom level."""
        if zoom == 5:
            return sst
        
        smoothed = self.smooth_sst(sst)
        scale = 20 if zoom == 8 else 30
        return self.increase_resolution(smoothed, scale)

    async def process_all_regions(self, date: datetime) -> Dict[str, List[Path]]:
        """Process all configured regions."""
        results = {}
        for source, config in settings.SOURCES.items():
            for region in config.regions:
                try:
                    paths = await self.process_region(date, region.value, source)
                    results[f"{source}_{region.value}"] = paths
                except Exception as e:
                    print(f"Error processing {source} {region}: {e}")
        return results
