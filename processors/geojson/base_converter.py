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

    def _round_coordinates(self, value: float, precision: int = 3) -> float:
        """Round coordinate values to specified precision."""
        return round(value, precision)

    def _optimize_feature(self, feature: dict, round_coords: bool = True) -> dict:
        """Optimize individual feature data."""
        if round_coords and feature['geometry']['type'] != 'LineString':
            # Round coordinates for points, but not for contour lines
            if isinstance(feature['geometry']['coordinates'][0], (int, float)):
                feature['geometry']['coordinates'] = [
                    self._round_coordinates(c) for c in feature['geometry']['coordinates']
                ]
            else:
                feature['geometry']['coordinates'] = [
                    [self._round_coordinates(c) for c in coord] 
                    for coord in feature['geometry']['coordinates']
                ]
        
        # Round numeric properties
        for key, value in feature['properties'].items():
            if isinstance(value, float):
                feature['properties'][key] = self._round_coordinates(value)
        
        return feature

    def save_geojson(self, geojson_data: dict, output_path: Path) -> None:
        """Save optimized GeoJSON data to file."""
        if output_path is None:
            return
        
        # Optimize features
        if 'features' in geojson_data:
            geojson_data['features'] = [
                self._optimize_feature(feature) 
                for feature in geojson_data['features']
            ]
        
        # Optimize metadata/properties
        if 'properties' in geojson_data:
            for key, value in geojson_data['properties'].items():
                if isinstance(value, dict):
                    for subkey, subvalue in value.items():
                        if isinstance(subvalue, float):
                            value[subkey] = self._round_coordinates(subvalue)
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(geojson_data, f, separators=(',', ':'))  # Minimize whitespace
        
        self.logger.info(f"💾 Generated optimized GeoJSON")
