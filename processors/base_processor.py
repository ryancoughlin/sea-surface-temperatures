import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from config.settings import IMAGE_SETTINGS, REGIONS_DIR
from config.regions import REGIONS
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

class BaseImageProcessor(ABC):
    """Base class for all image processors."""
    def __init__(self):
        self.settings = IMAGE_SETTINGS
        self.base_dir = REGIONS_DIR
        # Create land feature once during initialization
        self.land_feature = cfeature.NaturalEarthFeature(
            'physical', 'land', '10m',
            edgecolor='none',
            facecolor='none'
        )

    @abstractmethod
    def generate_image(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Tuple[Path, Optional[Dict]]:
        """
        Generate visualization and any additional layers.
        Returns:
            Tuple[Path, Optional[Dict]]: (image_path, additional_layers)
        """
        raise NotImplementedError

    def generate_image_path(self, region: str, dataset: str, timestamp: str) -> Path:
        """Generate standardized path for image storage."""
        path = self.base_dir / region / "datasets" / dataset / timestamp / "image.png"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def save_image(self, fig, region: str, dataset: str, timestamp: str) -> Path:
        """Save figure to standardized location."""
        try:
            image_path = self.generate_image_path(region, dataset, timestamp)
            
            if not hasattr(fig, 'savefig'):
                raise ValueError(f"Expected matplotlib figure, got {type(fig)}")
            
            # Save with transparency    
            fig.savefig(
                image_path,
                dpi=self.settings['dpi'],
                bbox_inches='tight',
                pad_inches=0,
                transparent=True
            )
            plt.close(fig)
            return image_path
        except Exception as e:
            logger.error(f"Error in save_image: {str(e)}")
            raise

    def create_masked_axes(self, region: str, figsize=(10, 8)) -> tuple[plt.Figure, plt.Axes]:
        """Create figure and axes with land areas masked."""
        bounds = REGIONS[region]['bounds']
        
        # Create figure with transparent background
        fig = plt.figure(figsize=figsize)
        fig.patch.set_alpha(0.0)
        
        # Create axes with Mercator projection
        ax = plt.axes(projection=ccrs.Mercator())
        ax.patch.set_alpha(0.0)
        
        # Set map extent
        ax.set_extent([
            bounds[0][0],  # west
            bounds[1][0],  # east
            bounds[0][1],  # south
            bounds[1][1]   # north
        ], crs=ccrs.PlateCarree())
        
        # Add the pre-created land feature
        ax.add_feature(self.land_feature)
        
        # Remove axes and padding
        ax.set_axis_off()
        plt.subplots_adjust(left=0, right=1, bottom=0, top=1)
        
        return fig, ax
