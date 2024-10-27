from abc import ABC, abstractmethod
from pathlib import Path
import logging
import xarray as xr
import numpy as np
import json
from config.settings import REGIONS_DIR

logger = logging.getLogger(__name__)

class BaseGeoJSONConverter(ABC):
    """Base class for GeoJSON converters with common functionality."""
    
    def __init__(self):
        self.base_dir = REGIONS_DIR
    
    def load_dataset(self, data_path: Path) -> xr.Dataset:
        """Common dataset loading with error handling."""
        try:
            return xr.open_dataset(data_path)
        except Exception as e:
            logger.error(f"Error loading dataset: {str(e)}")
            raise

    def get_coordinates(self, ds: xr.Dataset) -> tuple:
        """Extract longitude and latitude coordinates."""
        lon_name = 'longitude' if 'longitude' in ds.coords else 'lon'
        lat_name = 'latitude' if 'latitude' in ds.coords else 'lat'
        return ds[lon_name].values, ds[lat_name].values

    def select_time_slice(self, data: xr.DataArray) -> xr.DataArray:
        """Handle time dimension if present."""
        if 'time' in data.dims:
            return data.isel(time=0)
        return data

    def create_feature(self, lon: float, lat: float, properties: dict) -> dict:
        """Create a GeoJSON feature with given coordinates and properties."""
        return {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(lon), float(lat)]
            },
            "properties": properties
        }

    def save_geojson(self, geojson_data: dict, output_path: Path) -> None:
        """Save GeoJSON data to file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(geojson_data, f)
        logger.info(f"Generated GeoJSON file: {output_path}")

    def generate_geojson_path(self, region: str, dataset: str, timestamp: str) -> Path:
        """Generate standard output path for GeoJSON files."""
        path = self.base_dir / region / "datasets" / dataset / timestamp / "data.geojson"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @abstractmethod
    def convert(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Convert data to GeoJSON format."""
        pass
