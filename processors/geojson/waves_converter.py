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
            
            # Configuration
            decimation = export_config.get('decimation_factor', 4)
            min_height = export_config.get('min_height', 0.5)
            
            # Extract variables
            height = ds['VHM0'].isel(time=0)
            direction = ds['VMDR'].isel(time=0)
            mean_period = ds['VTM10'].isel(time=0)
            peak_period = ds['VTPK'].isel(time=0)
            
            # Get coordinates and mask
            lon_name, lat_name = self.get_coordinate_names(height)
            
            # Mask to region
            height = height.where(
                (height[lon_name] >= bounds[0][0]) & 
                (height[lon_name] <= bounds[1][0]) &
                (height[lat_name] >= bounds[0][1]) & 
                (height[lat_name] <= bounds[1][1]),
                drop=True
            )
            
            features = []
            for i in range(0, len(height[lat_name]), decimation):
                for j in range(0, len(height[lon_name]), decimation):
                    wave_height = float(height.values[i, j])
                    if np.isnan(wave_height) or wave_height < min_height:
                        continue
                        
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                float(height[lon_name][j]),
                                float(height[lat_name][i])
                            ]
                        },
                        "properties": {
                            "height": round(wave_height, 2),
                            "direction": round(float(direction.values[i, j]), 1),
                            "mean_period": round(float(mean_period.values[i, j]), 1),
                            "peak_period": round(float(peak_period.values[i, j]), 1),
                            "units": {
                                "height": "m",
                                "direction": "degrees",
                                "period": "seconds"
                            }
                        }
                    }
                    features.append(feature)
            
            ranges = {
                "height": {
                    "min": float(height.min()),
                    "max": float(height.max()),
                    "unit": "m"
                },
                "mean_period": {
                    "min": float(mean_period.min()),
                    "max": float(mean_period.max()),
                    "unit": "seconds"
                }
            }
            
            metadata = {
                "decimation_factor": decimation,
                "min_height": min_height
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