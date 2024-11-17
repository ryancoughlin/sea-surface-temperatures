import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from config.settings import IMAGE_SETTINGS
from config.regions import REGIONS
from typing import Dict, Optional, Tuple
from utils.path_manager import PathManager
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseImageProcessor(ABC):
    """Base class for all image processors."""
    def __init__(self, path_manager: PathManager):
        self.path_manager = path_manager
        self.settings = IMAGE_SETTINGS

    @abstractmethod
    def generate_image(self, data_path: Path, region: str, dataset: str, date: datetime) -> Tuple[Path, Optional[Dict]]:
        """
        Generate visualization and any additional layers.
        Returns:
            Tuple[Path, Optional[Dict]]: (image_path, additional_layers)
        """
        raise NotImplementedError

    def generate_image_path(self, region: str, dataset: str, date: datetime) -> Path:
        """Generate standardized path for image storage."""
        path = self.path_manager.get_asset_paths(date, dataset, region)
        return path.image

    def save_image(self, fig, region: str, dataset: str, date: datetime) -> Path:
        """Save figure with no padding or margins."""
        try:
            path = self.path_manager.get_asset_paths(date, dataset, region)
            
            fig.savefig(
                path.image,
                dpi=self.settings['dpi'],
                bbox_inches=None,  # Remove bbox_inches to prevent auto-padding
                pad_inches=0,      # Ensure no padding
                transparent=True,
                format='png'
            )
            plt.close(fig)
            return path.image
        except Exception as e:
            logger.error(f"Error saving image: {str(e)}")
            raise

    def create_axes(self, region: str) -> tuple[plt.Figure, plt.Axes]:
        """Create figure and axes with exact bounds."""
        bounds = REGIONS[region]['bounds']
        
        # Calculate aspect ratio from bounds
        lon_span = bounds[1][0] - bounds[0][0]
        lat_span = bounds[1][1] - bounds[0][1]
        aspect = lon_span / lat_span
        
        # Create figure with exact size ratio
        height = 24
        width = height * aspect
        
        # Create figure with no frame
        fig = plt.figure(figsize=(width, height), frameon=False)
        
        # Use PlateCarree projection instead of Mercator to match web map exactly
        ax = plt.axes([0, 0, 1, 1], 
                     projection=ccrs.PlateCarree())  # Simplified projection
        
        # Remove all axes elements and make background transparent
        ax.set_axis_off()
        ax.patch.set_alpha(0.0)
        fig.patch.set_alpha(0.0)
        
        # Remove Cartopy's gridlines
        gl = ax.gridlines()
        gl.remove()
        
        # Set exact bounds
        ax.set_extent([
            bounds[0][0],
            bounds[1][0],
            bounds[0][1],
            bounds[1][1]
        ], crs=ccrs.PlateCarree())
        
        return fig, ax
