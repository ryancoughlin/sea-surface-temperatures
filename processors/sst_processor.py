from pathlib import Path
import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
import logging
from .base_processor import BaseImageProcessor
from config.settings import SOURCES, REGIONS_DIR, IMAGE_SETTINGS
from config.regions import REGIONS
from utils.data_utils import convert_temperature_to_f
from utils.contour_utils import generate_temperature_contours, contours_to_geojson
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import json
from datetime import datetime
from typing import Dict

logger = logging.getLogger(__name__)

class SSTProcessor(BaseImageProcessor):
    def generate_image(self, data_path: Path, region: str, dataset: str, timestamp: str) -> tuple[Path, Dict]:
        """Generate SST image and contours."""
        try:
            dataset_dir = REGIONS_DIR / region / "datasets" / dataset / timestamp
            dataset_dir.mkdir(parents=True, exist_ok=True)
            
            image_path = dataset_dir / "image.png"
            contour_path = dataset_dir / "contours.geojson" if SOURCES[dataset]['category'] == 'sst' else None

            # Load data
            logger.info(f"Processing SST data for {region}")
            ds = xr.open_dataset(data_path)
            
            # Get variable name from settings
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            
            # Select first time slice if time dimension exists
            if 'time' in data.dims:
                logger.debug("Selecting first time slice from 3D data")
                data = data.isel(time=0)
            
            # Get coordinate names
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            
            # Get region bounds
            bounds = REGIONS[region]['bounds']
            
            # Mask to region using detected coordinate names
            lon_mask = (data[lon_name] >= bounds[0][0]) & (data[lon_name] <= bounds[1][0])
            lat_mask = (data[lat_name] >= bounds[0][1]) & (data[lat_name] <= bounds[1][1])
            regional_data = data.where(lon_mask & lat_mask, drop=True)
            
            # Convert temperature to Fahrenheit
            regional_data = convert_temperature_to_f(regional_data)
            
            # Create masked figure and axes
            fig, ax = self.create_masked_axes(region)
            
            # Plot filled contours
            filled_contours = ax.contourf(
                regional_data[lon_name],
                regional_data[lat_name],
                regional_data,
                levels=70,
                cmap=SOURCES[dataset]['color_scale'],
                extend='both',
                vmin=32,
                vmax=88,
                transform=ccrs.PlateCarree()
            )
            
            # Generate contour lines
            contour_lines = generate_temperature_contours(
                ax=ax,
                data=regional_data,
                lons=regional_data[lon_name],
                lats=regional_data[lat_name]
            )
            
            # Save image
            plt.savefig(image_path, dpi=IMAGE_SETTINGS['dpi'], bbox_inches='tight')
            plt.close()

            # Generate contours for SST only
            additional_layers = None
            if contour_path:
                contour_lines = generate_temperature_contours(
                    ax=ax,
                    data=regional_data,
                    lons=regional_data[lon_name],
                    lats=regional_data[lat_name]
                )
                
                contour_geojson = contours_to_geojson(contour_lines)
                
                with open(contour_path, 'w') as f:
                    json.dump(contour_geojson, f)

                additional_layers = {
                    "contours": {
                        "path": str(contour_path.relative_to(REGIONS_DIR)),
                        "type": "vector",
                        "style": {
                            "line-color": "#000",
                            "line-width": 1,
                            "line-opacity": 0.7
                        }
                    }
                }

            return image_path, additional_layers
            
        except Exception as e:
            logger.error(f"Error processing SST data: {str(e)}")
            raise
