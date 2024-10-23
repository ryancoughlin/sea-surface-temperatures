import numpy as np
import xarray as xr
from pathlib import Path
from typing import Tuple
from scipy.signal import savgol_filter
from scipy.interpolate import RegularGridInterpolator
from ...config.settings import settings

class ImageProcessor:
    def load_and_process(self, nc4_filepath: Path, source: str) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Load and process SST data."""
        sst, lat, lon = self._load_sst_data(nc4_filepath, source)
        return self._process_sst(sst), lat, lon

    def _load_sst_data(self, nc4_filepath: Path, source: str) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        with xr.open_dataset(nc4_filepath) as ds:
            # Get variable name from settings based on source
            var_name = settings.SOURCES[source].variables[0] if source == "erddap" else "sst"
            sst = ds[var_name].squeeze().values
            
            # Use full variable names for coordinates
            latitude = ds.latitude.values if 'latitude' in ds else ds.lat.values
            longitude = ds.longitude.values if 'longitude' in ds else ds.lon.values
            
        return sst, latitude, longitude

    def _process_sst(self, sst: np.ndarray) -> np.ndarray:
        """Convert to Fahrenheit and handle missing data."""
        sst_fahrenheit = (sst * 9/5) + 32
        return sst_fahrenheit

    def process_for_zoom(self, sst: np.ndarray, zoom: int) -> np.ndarray:
        """Process SST data for a specific zoom level."""
        if zoom == 5:
            return sst
        smoothed = self._smooth_sst(sst)
        scale = 20 if zoom == 8 else 30
        return self._increase_resolution(smoothed, scale)

    def _smooth_sst(self, sst: np.ndarray, window_length: int = 11, polyorder: int = 2) -> np.ndarray:
        smoothed = savgol_filter(sst, window_length=window_length, polyorder=polyorder, axis=0, mode='nearest')
        return savgol_filter(smoothed, window_length=window_length, polyorder=polyorder, axis=1, mode='nearest')

    def _increase_resolution(self, sst: np.ndarray, scale_factor: int) -> np.ndarray:
        original_grid = (np.arange(sst.shape[0]), np.arange(sst.shape[1]))
        interpolator = RegularGridInterpolator(original_grid, sst, bounds_error=False, fill_value=np.nan)
        new_y = np.linspace(0, sst.shape[0] - 1, sst.shape[0] * scale_factor)
        new_x = np.linspace(0, sst.shape[1] - 1, sst.shape[1] * scale_factor)
        new_grid = np.meshgrid(new_y, new_x, indexing='ij')
        return interpolator((new_grid[0], new_grid[1]))
