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
    
    def convert(self, data: xr.Dataset, region: str, dataset: str, date: datetime) -> Path:
        """
        Convert wave data to GeoJSON. Includes all available wave metrics.
        Returns data in user-friendly units (meters, seconds, degrees).
        """
        try:
            # Get coordinates and handle time dimension
            height = data['VHM0']
            if 'time' in height.dims:
                height = height.isel(time=0)
            
            # Get coordinate names
            lon_name, lat_name = self.get_coordinate_names(height)
            
            # Check which variables are available
            has_direction = 'VMDR' in data
            has_mean_period = 'VTM10' in data
            has_peak_period = 'VTPK' in data
            
            # Load optional variables if available and handle time dimension
            if has_direction:
                direction = data['VMDR']
                if 'time' in direction.dims:
                    direction = direction.isel(time=0)
            if has_mean_period:
                mean_period = data['VTM10']
                if 'time' in mean_period.dims:
                    mean_period = mean_period.isel(time=0)
            if has_peak_period:
                peak_period = data['VTPK']
                if 'time' in peak_period.dims:
                    peak_period = peak_period.isel(time=0)
            
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
            
            if has_direction:
                ranges["direction"] = {
                    "min": float(direction.min()),
                    "max": float(direction.max()),
                    "unit": "degrees"
                }
            
            if has_mean_period:
                ranges["mean_period"] = {
                    "min": float(mean_period.min()),
                    "max": float(mean_period.max()),
                    "unit": "seconds"
                }
            
            if has_peak_period:
                ranges["peak_period"] = {
                    "min": float(peak_period.min()),
                    "max": float(peak_period.max()),
                    "unit": "seconds"
                }
            
            metadata = {
                "available_variables": ["height"] + 
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
            
            output_path = self.path_manager.get_asset_paths(date, dataset, region).data
            return self.save_geojson(geojson, output_path)
            
        except Exception as e:
            logger.error(f"Error converting waves to GeoJSON: {str(e)}")
            raise