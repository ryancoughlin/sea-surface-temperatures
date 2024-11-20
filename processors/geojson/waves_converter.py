from pathlib import Path
from datetime import datetime
import xarray as xr
import numpy as np
import logging
from typing import Dict, List
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from config.regions import REGIONS
from utils.data_utils import calculate_wave_energy, calculate_wave_steepness

logger = logging.getLogger(__name__)

class WavesGeoJSONConverter(BaseGeoJSONConverter):
    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert wave data to GeoJSON with vector and scalar properties."""
        try:
            ds = self.load_dataset(data_path)
            bounds = REGIONS[region]['bounds']
            dataset_config = SOURCES[dataset]
            export_config = dataset_config.get('export_geojson', {})
            
            # Configuration for vector decimation
            decimation = export_config.get('decimation_factor', 4)
            min_height = export_config.get('min_height', 0.5)
            
            # Extract variables
            height = ds['VHM0'].isel(time=0)
            direction = ds['VMDR'].isel(time=0)
            mean_period = ds['VTM10'].isel(time=0)
            peak_period = ds['VTPK'].isel(time=0)
            energy_period = ds['VPED'].isel(time=0)
            
            # Calculate derived parameters
            wave_energy = calculate_wave_energy(height, mean_period)
            wave_steepness = calculate_wave_steepness(height, mean_period)
            
            # Get standardized coordinate names
            lon_name, lat_name = self.get_coordinate_names(height)
            
            # Mask to region
            lon_mask = (height[lon_name] >= bounds[0][0]) & (height[lon_name] <= bounds[1][0])
            lat_mask = (height[lat_name] >= bounds[0][1]) & (height[lat_name] <= bounds[1][1])
            
            # Apply masks to all variables
            height = height.where(lon_mask & lat_mask, drop=True)
            direction = direction.where(lon_mask & lat_mask, drop=True)
            mean_period = mean_period.where(lon_mask & lat_mask, drop=True)
            peak_period = peak_period.where(lon_mask & lat_mask, drop=True)
            energy_period = energy_period.where(lon_mask & lat_mask, drop=True)
            wave_energy = wave_energy.where(lon_mask & lat_mask, drop=True)
            wave_steepness = wave_steepness.where(lon_mask & lat_mask, drop=True)
            
            features = []
            for i in range(0, len(height[lat_name]), decimation):
                for j in range(0, len(height[lon_name]), decimation):
                    try:
                        wave_height = float(height.values[i, j])
                        
                        if np.isnan(wave_height) or wave_height < min_height:
                            continue
                            
                        # Convert direction to vector components for visualization
                        dir_val = float(direction.values[i, j])
                        u = -np.sin(np.deg2rad(dir_val))
                        v = -np.cos(np.deg2rad(dir_val))
                        
                        feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [
                                    round(float(height[lon_name].values[j]), 2),
                                    round(float(height[lat_name].values[i]), 2)
                                ]
                            },
                            "properties": {
                                "height": round(wave_height, 2),
                                "direction": round(dir_val, 2),
                                "u": round(u, 2),
                                "v": round(v, 2),
                                "mean_period": round(float(mean_period.values[i, j]), 2),
                                "peak_period": round(float(peak_period.values[i, j]), 2),
                                "energy_period": round(float(energy_period.values[i, j]), 2),
                                "wave_energy": round(float(wave_energy.values[i, j]), 2),
                                "wave_steepness": round(float(wave_steepness.values[i, j]), 2),
                                "base": 0,  # For 3D extrusion
                                "height_category": self._get_height_category(wave_height)
                            }
                        }
                        features.append(feature)
                        
                    except Exception as e:
                        logger.warning(f"Error processing wave point ({i},{j}): {e}")
                        continue
            
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "date": date.strftime('%Y-%m-%d'),
                    "decimation_factor": decimation,
                    "min_height": min_height,
                    "bounds": {
                        "min_lon": round(float(height[lon_name].min()), 2),
                        "max_lon": round(float(height[lon_name].max()), 2),
                        "min_lat": round(float(height[lat_name].min()), 2),
                        "max_lat": round(float(height[lat_name].max()), 2)
                    },
                    "ranges": {
                        "height": {
                            "min": round(float(height.min()), 2),
                            "max": round(float(height.max()), 2)
                        },
                        "energy": {
                            "min": round(float(wave_energy.min()), 2),
                            "max": round(float(wave_energy.max()), 2)
                        }
                    }
                }
            }
            
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            self.save_geojson(geojson, asset_paths.data)
            return asset_paths.data
            
        except Exception as e:
            logger.error(f"Error converting waves to GeoJSON: {str(e)}")
            raise
            
    def _get_height_category(self, height: float) -> str:
        """Categorize wave heights for visualization and filtering."""
        if height < 2:
            return "calm"
        elif height < 4:
            return "moderate"
        elif height < 6:
            return "rough"
        else:
            return "dangerous"