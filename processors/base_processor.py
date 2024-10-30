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
        self.land_feature = cfeature.NaturalEarthFeature(
            'physical', 'land', '10m',
            edgecolor='none',
            facecolor='none'
        )

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
        """Save figure with zero padding."""
        try:
            path = self.path_manager.get_asset_paths(date, dataset, region)
            
            # Save with zero padding and transparency
            fig.savefig(
                path.image,
                dpi=self.settings['dpi'],
                bbox_inches=None,  # Disable bbox_inches to prevent padding
                pad_inches=0,      # Explicitly set padding to 0
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
        
        # Create figure with transparent background
        fig = plt.figure(figsize=figsize, frameon=False)
        fig.patch.set_alpha(0.0)
        
        # Create axes that fills entire figure with zero padding
        ax = plt.axes([0, 0, 1, 1], projection=ccrs.Mercator())
        ax.patch.set_alpha(0.0)
        
        # Set map extent
        ax.set_extent([
            bounds[0][0],  # west
            bounds[1][0],  # east
            bounds[0][1],  # south
            bounds[1][1]   # north
        ], crs=ccrs.PlateCarree())
        
        # Add land feature and remove all axes elements
        ax.add_feature(self.land_feature)
        ax.set_axis_off()
        
        return fig, ax
