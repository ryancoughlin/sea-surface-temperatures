import xarray as xr
import numpy as np
from pathlib import Path
from scipy.signal import savgol_filter
from scipy.interpolate import RegularGridInterpolator
from typing import Tuple
from ..tiles.generator import TileGenerator

class SSTProcessor:
    """Handles all SST data processing and image generation."""
    
    def __init__(self):
        self.tile_generator = TileGenerator()
        
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

    async def process_file(self, input_file: Path, output_dir: Path) -> list[Path]:
        """Process a single SST file."""
        print(f"Processing {input_file}")
        
        # Load and convert data
        sst, lat, lon = self.load_sst_data(input_file)
        
        # Process different zoom levels
        tile_paths = []
        zoom_levels = [5, 8, 10]
        
        for zoom in zoom_levels:
            if zoom == 5:
                output_sst = sst
            elif zoom == 8:
                smoothed = self.smooth_sst(sst)
                output_sst = self.increase_resolution(smoothed, 20)
            elif zoom == 10:
                smoothed = self.smooth_sst(sst)
                output_sst = self.increase_resolution(smoothed, 30)
            
            paths = self.tile_generator.generate_tiles(
                output_sst, lat, lon, zoom, output_dir
            )
            tile_paths.extend(paths)
            
        return tile_paths
