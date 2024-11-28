from pathlib import Path
from datetime import datetime
import xarray as xr
import numpy as np
import logging
from typing import Dict, List
from .base_converter import BaseGeoJSONConverter

logger = logging.getLogger(__name__)

class WavesGeoJSONConverter(BaseGeoJSONConverter):
    """Converts wave data to GeoJSON format with available wave metrics"""
    
    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """
        Convert wave data to GeoJSON. Includes all available wave metrics.
        Returns data in user-friendly units (meters, seconds, degrees).
        """
        try:
            ds = self.load_dataset(data_path)
            
            # Get coordinates
            height = ds['VHM0'].isel(time=0)
            lon_name, lat_name = self.get_coordinate_names(height)
            
            # Check which variables are available
            has_direction = 'VMDR' in ds
            has_mean_period = 'VTM10' in ds
            has_peak_period = 'VTPK' in ds
            
            # Load optional variables if available
            if has_direction:
                direction = ds['VMDR'].isel(time=0)
            if has_mean_period:
                mean_period = ds['VTM10'].isel(time=0)
            if has_peak_period:
                peak_period = ds['VTPK'].isel(time=0)
            
            features = []
            for i in range(len(height[lat_name])):
                for j in range(len(height[lon_name])):
                    wave_height = float(height.values[i, j])
                    if np.isnan(wave_height):
                        continue
                    
                    # Build properties starting with wave height
                    properties = {
                        "height": round(wave_height, 2),
                        "units": {
                            "height": "m"
                        }
                    }
                    
                    # Add other wave metrics if available
                    if has_direction:
                        dir_value = float(direction.values[i, j])
                        if not np.isnan(dir_value):
                            properties["direction"] = round(dir_value, 1)
                            properties["units"]["direction"] = "degrees"
                            
                    if has_mean_period:
                        period_value = float(mean_period.values[i, j])
                        if not np.isnan(period_value):
                            properties["mean_period"] = round(period_value, 1)
                            properties["units"]["period"] = "seconds"
                            
                    if has_peak_period:
                        peak_value = float(peak_period.values[i, j])
                        if not np.isnan(peak_value):
                            properties["peak_period"] = round(peak_value, 1)
                    
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                float(height[lon_name][j]),
                                float(height[lat_name][i])
                            ]
                        },
                        "properties": properties
                    }
                    features.append(feature)
            
            # Calculate ranges for available variables
            ranges = {
                "height": {
                    "min": float(height.min()),
                    "max": float(height.max()),
                    "unit": "m"
                }
            }
            
            if has_mean_period:
                ranges["mean_period"] = {
                    "min": float(mean_period.min()),
                    "max": float(mean_period.max()),
                    "unit": "seconds"
                }
            
            metadata = {
                "variables": ["height"] + 
                    (["direction"] if has_direction else []) +
                    (["mean_period"] if has_mean_period else []) +
                    (["peak_period"] if has_peak_period else [])
            }
            
            geojson = self.create_standardized_geojson(
                features=features,
                date=date,
                dataset=dataset,
                ranges=ranges,
                metadata=metadata
            )
            
            return self.save_geojson(
                geojson,
                self.path_manager.get_asset_paths(date, dataset, region).data
            )
            
        except Exception as e:
            logger.error(f"Error converting waves to GeoJSON: {str(e)}")
            raise