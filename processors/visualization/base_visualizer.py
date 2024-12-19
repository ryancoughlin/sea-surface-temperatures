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
from PIL import Image
from io import BytesIO
import numpy as np
import xarray as xr
import scipy.ndimage
from utils.image_optimizer import ImageOptimizer
from processors.data.data_utils import get_coordinate_names

logger = logging.getLogger(__name__)

class BaseVisualizer(ABC):
    """Base class for all data visualizers."""
    def __init__(self, path_manager: PathManager):
        self.path_manager = path_manager
        self.settings = IMAGE_SETTINGS
        self.image_optimizer = ImageOptimizer()

    @abstractmethod
    def generate_image(self, data: xr.DataArray | xr.Dataset, region: str, dataset: str, date: datetime) -> Tuple[Path, Optional[Dict]]:
        """Generate visualization and any additional layers."""
        raise NotImplementedError

    def get_coordinate_names(self, dataset):
        """Get the longitude and latitude variable names from the dataset."""
        return get_coordinate_names(dataset)

    def generate_image_path(self, region: str, dataset: str, date: datetime) -> Path:
        """Generate standardized path for image storage."""
        path = self.path_manager.get_asset_paths(date, dataset, region)
        return path.image

    def save_image(self, data: xr.DataArray | xr.Dataset, region: str, dataset: str, date: datetime, asset_paths=None) -> Path:
        """Save figure with optimization."""
        try:
            # Generate the visualization
            fig, _ = self.generate_image(data, region, dataset, date)
            
            # Get paths
            if asset_paths is None:
                asset_paths = self.path_manager.get_asset_paths(date, dataset, region)
            
            # Ensure directory exists
            asset_paths.image.parent.mkdir(parents=True, exist_ok=True)
            
            # Save to BytesIO first to avoid PIL fileno error
            buf = BytesIO()
            fig.savefig(
                buf,
                dpi=self.settings['dpi'],
                bbox_inches='tight',
                format='png'
            )
            plt.close(fig)
            
            # Write buffer to file
            buf.seek(0)
            with open(asset_paths.image, 'wb') as f:
                f.write(buf.getvalue())
            
            # Optimize the saved image
            self.image_optimizer.optimize_png(asset_paths.image)
            
            return asset_paths.image
            
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
        
        # Use PlateCarree projection
        ax = plt.axes([0, 0, 1, 1], projection=ccrs.PlateCarree())
        
        # Remove all axes elements and make background transparent
        ax.set_axis_off()
        ax.patch.set_alpha(0.0)
        fig.patch.set_alpha(0.0)
        
        # Set exact bounds  
        ax.set_extent([
            bounds[0][0],
            bounds[1][0],
            bounds[0][1],
            bounds[1][1]
        ], crs=ccrs.PlateCarree())
        
        return fig, ax

    def expand_coastal_data(self, data: xr.DataArray, buffer_size: int = 3) -> xr.DataArray:
        """
        Expands data near coastlines using efficient rolling window operations.
        
        Args:
            data: Input DataArray
            buffer_size: Number of cells to expand (default 3)
        """
        expanded_data = data.copy()
        
        for _ in range(buffer_size):
            # Create rolling window view of the data
            rolled = expanded_data.rolling(
                {dim: 3 for dim in expanded_data.dims[-2:]}, 
                center=True, 
                min_periods=1
            )
            
            # Calculate mean of surrounding cells
            filled = rolled.mean()
            
            # Only update NaN values where surrounding cells have data
            mask = np.isnan(expanded_data) & ~np.isnan(filled)
            if not np.any(mask):
                break
            
            expanded_data = xr.where(mask, filled, expanded_data)
        
        return expanded_data

