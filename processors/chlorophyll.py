from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import logging
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS
import xarray as xr

logger = logging.getLogger(__name__)

class ChlorophyllProcessor(BaseImageProcessor):
    def generate_image(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Generate chlorophyll visualization."""
        try:
            # Load data
            ds = xr.open_dataset(data_path)
            bounds = REGIONS[region]['bounds']
            
            # Get variable name from settings
            var_name = SOURCES[dataset]['variables'][0]
            
            # Get chlorophyll data
            chlor = ds[var_name]
            
            # Select first time slice if time dimension exists
            if 'time' in chlor.dims:
                logger.debug("Selecting first time slice from 3D data")
                chlor = chlor.isel(time=0)
            
            # Subset data after time selection
            ds_subset = chlor.sel(
                longitude=slice(bounds[0][0], bounds[1][0]),
                latitude=slice(bounds[0][1], bounds[1][1])
            )
            
            # Create figure
            fig, ax = plt.subplots(figsize=(10, 8), facecolor='none')
            ax.set_facecolor('none')
            
            # Plot chlorophyll concentration
            im = ax.contourf(
                ds_subset.longitude,
                ds_subset.latitude,
                ds_subset,
                levels=np.linspace(0, 20, 50),  # Adjust range for chlorophyll
                cmap=SOURCES[dataset]['color_scale'],
                extend='both'
            )
            
            # Clean up plot
            ax.axis('off')
            plt.tight_layout(pad=0)
            
            # Save image
            image_path = self.generate_image_path(region, dataset, timestamp)
            image_path.parent.mkdir(parents=True, exist_ok=True)
            
            fig.savefig(
                image_path,
                dpi=300,
                bbox_inches='tight',
                transparent=True,
                pad_inches=0
            )
            plt.close(fig)
            
            logger.info(f"Chlorophyll image saved to {image_path}")
            return image_path
            
        except Exception as e:
            logger.error(f"Error processing chlorophyll data: {str(e)}")
            raise
