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
        # Keep land feature but configure it for masking only
        self.land_feature = cfeature.LAND.with_scale('10m')
        self.ocean_feature = cfeature.OCEAN.with_scale('10m')

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
        """Save figure with zero padding and ensure land masking."""
        try:
            path = self.path_manager.get_asset_paths(date, dataset, region)
            
            # Get the current axes
            ax = plt.gca()
            
            # Ensure land mask is applied with proper z-order
            ax.add_feature(self.land_feature, facecolor='none', edgecolor='none', zorder=2)
            
            # Ensure correct extent
            bounds = REGIONS[region]['bounds']
            ax.set_extent([
                bounds[0][0],
                bounds[1][0],
                bounds[0][1],
                bounds[1][1]
            ], crs=ccrs.PlateCarree())
            
            # Save with zero padding and transparency
            fig.savefig(
                path.image,
                dpi=self.settings['dpi'],
                bbox_inches='tight',
                pad_inches=0,
                transparent=True,
                format='png'
            )
            plt.close(fig)
            return path.image
        except Exception as e:
            logger.error(f"Error saving image: {str(e)}")
            raise

    def create_masked_axes(self, region: str, figsize=(10, 8)) -> tuple[plt.Figure, plt.Axes]:
        """Create figure and axes with zero padding."""
        bounds = REGIONS[region]['bounds']
        
        # Calculate aspect ratio from bounds
        lon_span = bounds[1][0] - bounds[0][0]
        lat_span = bounds[1][1] - bounds[0][1]
        aspect = lon_span / lat_span
        
        # Adjust figsize to match aspect ratio
        height = 8  # base height
        width = height * aspect
        
        fig = plt.figure(figsize=(width, height), frameon=False)
        fig.patch.set_alpha(0.0)
        
        # Calculate center longitude for projection
        center_lon = (bounds[0][0] + bounds[1][0]) / 2
        ax = plt.axes([0, 0, 1, 1], 
                     projection=ccrs.Mercator(central_longitude=center_lon))
        ax.patch.set_alpha(0.0)
        
        # Set map extent
        ax.set_extent([
            bounds[0][0],  # west
            bounds[1][0],  # east
            bounds[0][1],  # south
            bounds[1][1]   # north
        ], crs=ccrs.PlateCarree())
        
        # Add land feature but make it transparent
        ax.add_feature(self.land_feature, facecolor='none', edgecolor='none')
        ax.set_axis_off()
        
        return fig, ax
