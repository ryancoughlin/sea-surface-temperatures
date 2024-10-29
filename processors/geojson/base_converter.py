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
    
    def load_dataset(self, data_path: Path) -> xr.Dataset:
        """Common dataset loading with error handling."""
        try:
            logger.info(f"Loading dataset from: {data_path}")
            return xr.open_dataset(data_path)
        except Exception as e:
            logger.error(f"Error loading dataset: {str(e)}")
            raise

    def save_geojson(self, geojson_data: dict, output_path: Path) -> None:
        """Save GeoJSON data to file."""
        if output_path is None:
            return
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(geojson_data, f)
        logger.info(f"Generated GeoJSON file: {output_path}")

    @abstractmethod
    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert data to GeoJSON format."""
        pass

    def select_time_slice(self, data: xr.DataArray) -> xr.DataArray:
        """Select first time slice if time dimension exists."""
        if 'time' in data.dims:
            logger.debug("Selecting first time slice from 3D data")
            return data.isel(time=0)
        return data

    def create_feature(self, lon: float, lat: float, properties: dict) -> dict:
        """Create a GeoJSON feature."""
        return {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": properties
        }
