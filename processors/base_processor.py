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
        """Save figure to standardized location with absolutely no padding."""
        try:
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            
            if not hasattr(fig, 'savefig'):
                raise ValueError(f"Expected matplotlib figure, got {type(fig)}")
            
            # Save with zero padding and transparency
            fig.savefig(
                asset_paths.image,
                dpi=self.settings['dpi'],
                bbox_inches=None,     # Don't use bbox_inches='tight' as it adds padding
                pad_inches=0,         # Explicitly set padding to 0
                transparent=True,     # Ensure transparency
                format='png'          # Explicitly set format
            )
            plt.close(fig)
            return asset_paths.image
        except Exception as e:
            logger.error(f"Error in save_image: {str(e)}")
            raise

    def create_masked_axes(self, region: str, figsize=(10, 8)) -> tuple[plt.Figure, plt.Axes]:
        """Create figure and axes with land areas masked and no padding."""
        bounds = REGIONS[region]['bounds']
        
        # Create figure with zero padding and transparent background
        fig = plt.figure(figsize=figsize, frameon=False)
        fig.patch.set_alpha(0.0)
        
        # Create axes that fills the entire figure with zero padding
        ax = plt.axes([0, 0, 1, 1], projection=ccrs.Mercator(), frameon=False)
        ax.patch.set_alpha(0.0)
        
        # Set map extent
        ax.set_extent([
            bounds[0][0],  # west
            bounds[1][0],  # east
            bounds[0][1],  # south
            bounds[1][1]   # north
        ], crs=ccrs.PlateCarree())
        
        # Add land feature
        ax.add_feature(self.land_feature)
        
        # Remove all axes elements and padding
        ax.set_axis_off()
        ax.set_frame_on(False)
        
        # Remove all margins and spacing
        plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
        plt.margins(0, 0)
        
        return fig, ax
