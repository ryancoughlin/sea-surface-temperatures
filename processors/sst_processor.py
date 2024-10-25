from pathlib import Path
import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
import logging
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.settings import OUTPUT_DIR
from config.regions import REGIONS
from utils.data_utils import convert_temperature_to_f, interpolate_data
from .metadata_assembler import MetadataAssembler

logger = logging.getLogger(__name__)

class SSTProcessor(BaseImageProcessor):
    def generate_image(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Generate SST image for a specific region."""
        try:
            # Load data
            logger.info(f"Processing SST data for {region}")
            ds = xr.open_dataset(data_path)
            
            # Get variable name from settings
            var_name = SOURCES[dataset]['variable'][0]  # Get first variable since SST only has one
            data = ds[var_name]
            
            # Select first time slice if time dimension exists
            if 'time' in data.dims:
                logger.debug("Selecting first time slice from 3D data")
                data = data.isel(time=0)
            
            # Get coordinate names (they might be longitude/latitude or lon/lat)
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            
            logger.debug(f"Using coordinate names: {lon_name}, {lat_name}")
            logger.debug(f"Data dimensions after time selection: {data.dims}")
            
            # Get region bounds
            bounds = REGIONS[region]['bounds']
            logger.info(f"Region bounds: {bounds}")
            
            # Mask to region using detected coordinate names
            lon_mask = (data[lon_name] >= bounds[0][0]) & (data[lon_name] <= bounds[1][0])
            lat_mask = (data[lat_name] >= bounds[0][1]) & (data[lat_name] <= bounds[1][1])
            regional_data = data.where(lon_mask & lat_mask, drop=True)
            
            # Convert temperature to Fahrenheit with auto-detection
            regional_data = convert_temperature_to_f(regional_data)
            
            # Interpolate data
            data_interpolated = interpolate_data(regional_data, factor=2)
            
            # Create figure
            fig, ax = plt.subplots(figsize=(10, 8))
            
            # Plot data
            contour = ax.contourf(
                data_interpolated,
                levels=70,
                cmap=SOURCES[dataset]['color_scale'],
                extend='both',
                vmin=32,
                vmax=88
            )
            
            # Add contour lines
            ax.contour(
                data_interpolated,
                colors='black',
                alpha=0.2,
                linewidths=0.5,
                levels=5
            )
            
            # Finalize plot
            ax.axis('off')
            plt.tight_layout(pad=0)
            
            # Generate image path
            image_path = OUTPUT_DIR / "images" / region / dataset / f"{dataset}_{region}_{timestamp}.png"
            
            # Ensure directory exists
            image_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save image
            fig.savefig(image_path, dpi=self.settings['dpi'], bbox_inches='tight')
            plt.close(fig)
            
            logger.info(f"SST image saved to {image_path}")
            return image_path
            
        except Exception as e:
            logger.error(f"Error processing SST data: {str(e)}")
            raise
