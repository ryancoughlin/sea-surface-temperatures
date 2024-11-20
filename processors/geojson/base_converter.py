from abc import ABC, abstractmethod
from pathlib import Path
import logging
import xarray as xr
import numpy as np
import json
from utils.path_manager import PathManager
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseGeoJSONConverter(ABC):
    """Base class for GeoJSON converters with common functionality."""
    
    def __init__(self, path_manager: PathManager):
        self.path_manager = path_manager
        self.logger = logging.getLogger(__name__)
    
    def load_dataset(self, data_path: Path) -> xr.Dataset:
        """Common dataset loading with error handling."""
        try:
            self.logger.info(f"📂 Loading dataset")
            return xr.open_dataset(data_path)
        except Exception as e:
            self.logger.error(f"❌ Error loading dataset")
            self.logger.error(f"   └── 💥 {str(e)}")
            raise
    
    def normalize_dataset(self, ds: xr.Dataset, var_name: str) -> xr.DataArray:
        """Normalize dataset structure by handling different dimension layouts."""
        data = ds[var_name]
        
        # Handle time dimension
        if 'time' in data.dims:
            data = data.isel(time=0)
        
        # Handle depth dimension if present (CMEMS data)
        if 'depth' in data.dims:
            data = data.isel(depth=0)
        
        # Handle altitude dimension if present
        if 'altitude' in data.dims:
            data = data.isel(altitude=0)
        
        return data
    
    def get_coordinate_names(self, data: xr.DataArray) -> tuple:
        """Get standardized coordinate names."""
        lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
        lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
        return lon_name, lat_name

    def save_geojson(self, geojson_data: dict, output_path: Path) -> None:
        """Save GeoJSON data to file."""
        if output_path is None:
            return
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(geojson_data, f)
        self.logger.info(f"💾 Generated GeoJSON")
