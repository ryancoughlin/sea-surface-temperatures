from abc import ABC, abstractmethod
from pathlib import Path
import logging
import xarray as xr
import numpy as np
import json
from typing import Callable, Dict, List, Optional, Tuple, Union, Any
from utils.path_manager import PathManager
from datetime import datetime
from config.settings import SOURCES
from processors.data.data_utils import get_coordinate_names

logger = logging.getLogger(__name__)

class BaseGeoJSONConverter(ABC):
    """Base class for GeoJSON converters with common functionality."""
    
    def __init__(self, path_manager: PathManager, metadata_assembler=None):
        self.path_manager = path_manager
        self.metadata_assembler = metadata_assembler
        self.logger = logging.getLogger(__name__)
    
    @abstractmethod
    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
        """Convert data to GeoJSON format.
        
        Args:
            data: Input data as xarray Dataset
            region: Region identifier
            dataset: Dataset identifier
            date: Processing date
            
        Returns:
            Path to generated GeoJSON file
        """
        pass
    
    def get_coordinate_names(self, data: xr.Dataset) -> tuple:
        """Get standardized coordinate names."""
        lon_patterns = ['lon', 'longitude', 'x']
        lat_patterns = ['lat', 'latitude', 'y']
        
        lon_name = None
        lat_name = None
        
        for var in data.coords:
            var_lower = var.lower()
            if any(pattern in var_lower for pattern in lon_patterns):
                lon_name = var
            elif any(pattern in var_lower for pattern in lat_patterns):
                lat_name = var
                
        if not lon_name or not lat_name:
            raise ValueError("Could not identify coordinate variables")
            
        return lon_name, lat_name

    def _generate_features(self, 
                         lats: np.ndarray, 
                         lons: np.ndarray,
                         property_generator: Callable[[int, int], Optional[Dict]]) -> List[Dict]:
        """
        Generate GeoJSON features using a property generator callback.
        
        Args:
            lats: Latitude values array
            lons: Longitude values array
            property_generator: Callback function that takes (i, j) indices and returns properties dict or None
            
        Returns:
            List of GeoJSON features
        """
        features = []
        for i in range(len(lats)):
            for j in range(len(lons)):
                properties = property_generator(i, j)
                if properties is None:  # Skip if any values are NaN
                    continue
                    
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            self._round_coordinates(float(lons[j])),
                            self._round_coordinates(float(lats[i]))
                        ]
                    },
                    "properties": properties
                }
                features.append(feature)
        return features

    def _calculate_ranges(self, data_dict: Dict[str, Tuple[np.ndarray, str]]) -> Dict[str, Dict]:
        """
        Calculate ranges for multiple variables.
        
        Args:
            data_dict: Dictionary mapping variable names to (data, unit) tuples
            
        Returns:
            Dictionary of ranges with min, max, and unit for each variable
        """
        ranges = {}
        for var_name, (data, unit) in data_dict.items():
            valid_data = data[~np.isnan(data)]
            if len(valid_data) > 0:
                ranges[var_name] = {
                    "min": float(np.min(valid_data)),
                    "max": float(np.max(valid_data)),
                    "unit": unit
                }
        return ranges

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

    def save_geojson(self, geojson_data: dict, output_path: Path) -> Path:
        """Save optimized GeoJSON data to file."""
        if output_path is None:
            logger.error("No output path provided")
            return None
        
        try:
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
            
            # Ensure directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save file
            with open(output_path, 'w') as f:
                json.dump(geojson_data, f, separators=(',', ':'))  # Minimize whitespace
            
            logger.info(f"💾 Generated GeoJSON: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to save GeoJSON: {e}")
            return None

    def create_standardized_geojson(self, features: list, date: datetime, 
                               dataset: str, ranges: dict, metadata: dict) -> dict:
        """Create a standardized GeoJSON object."""
        return {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "date": date.strftime('%Y%m%d'),
                "dataset": dataset,
                "ranges": ranges,
                **metadata
            }
        }

    def save_empty_geojson(self, date: datetime, dataset: str, region: str) -> Path:
        """Save an empty GeoJSON when no features can be generated."""
        empty_geojson = self.create_standardized_geojson(
            features=[],
            date=date,
            dataset=dataset,
            ranges={},
            metadata={"status": "no_features_generated"}
        )
        
        asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
        return self.save_geojson(empty_geojson, asset_paths.contours)
