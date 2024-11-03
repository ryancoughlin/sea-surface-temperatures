from pathlib import Path
import matplotlib.pyplot as plt
import xarray as xr
import logging
import cartopy.crs as ccrs
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS
from utils.data_utils import convert_temperature_to_f

logger = logging.getLogger(__name__)

class SSTProcessor(BaseImageProcessor):
    def __init__(self, path_manager):
        super().__init__(path_manager)

    def generate_image(self, data_path: Path, region: str, dataset: str, date: str) -> Path:
        """Generate SST visualization."""
        try:
            # Get paths and create directories
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            asset_paths.image.parent.mkdir(parents=True, exist_ok=True)

            # Load and process data
            ds = xr.open_dataset(data_path)
            data = ds[SOURCES[dataset]['variables'][0]]
            
            # Handle time dimension if present
            if 'time' in data.dims:
                data = data.isel(time=0)
            
            # Get coordinates and bounds
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            bounds = REGIONS[region]['bounds']
            
            # Mask to region
            regional_data = data.where(
                (data[lon_name] >= bounds[0][0]) & 
                (data[lon_name] <= bounds[1][0]) &
                (data[lat_name] >= bounds[0][1]) & 
                (data[lat_name] <= bounds[1][1]), 
                drop=True
            )
            
            # Convert temperature and create visualization
            regional_data = convert_temperature_to_f(regional_data)
            
            # Create figure and axes
            fig, ax = self.create_masked_axes(region)
            
            # Plot filled contours for ocean data
            contour = ax.contourf(
                regional_data[lon_name],
                regional_data[lat_name],
                regional_data,
                levels=50,
                cmap=SOURCES[dataset]['color_scale'],
                extend='both',
                vmin=36,
                vmax=82,
                transform=ccrs.PlateCarree(),
                zorder=1,
                antialiased=True
            )
            
            return self.save_image(fig, region, dataset, date)
            
        except Exception as e:
            logger.error(f"Error processing SST data: {str(e)}")
            raise
