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
import subprocess
from io import BytesIO
import numpy as np
import xarray as xr
import scipy.ndimage
from utils.image_optimizer import ImageOptimizer

logger = logging.getLogger(__name__)

class BaseImageProcessor(ABC):
    """Base class for all image processors."""
    def __init__(self, path_manager: PathManager):
        self.path_manager = path_manager
        self.settings = IMAGE_SETTINGS
        self.image_optimizer = ImageOptimizer()

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
        """Save figure with optimization."""
        try:
            path = self.path_manager.get_asset_paths(date, dataset, region)
            path.image.parent.mkdir(parents=True, exist_ok=True)
            
            # Save initial high-quality image
            fig.savefig(
                path.image,
                dpi=self.settings['dpi'],
                bbox_inches='tight',
                format='png'
            )
            plt.close(fig)
            
            # Optimize the saved image
            self.image_optimizer.optimize_png(path.image)
            
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

    def get_coordinate_names(self, dataset):
        """Get the longitude and latitude variable names from the dataset."""
        # Common coordinate name patterns
        lon_patterns = ['lon', 'longitude', 'x']
        lat_patterns = ['lat', 'latitude', 'y']
        
        # Find coordinate names
        lon_name = None
        lat_name = None
        
        for var in dataset.coords:
            var_lower = var.lower()
            if any(pattern in var_lower for pattern in lon_patterns):
                lon_name = var
            elif any(pattern in var_lower for pattern in lat_patterns):
                lat_name = var
                
        if not lon_name or not lat_name:
            raise ValueError("Could not identify coordinate variables")
            
        return lon_name, lat_name

    
    def expand_coastal_data(self, data: xr.DataArray, buffer_size: int = 3) -> xr.DataArray:
        """
        Expands data near coastlines to prevent gaps while preserving original values.
        
        Args:
            data: Input DataArray
            buffer_size: Number of cells to expand (default 3)
        """
        # Create a mask of valid (non-NaN) data
        valid_mask = ~np.isnan(data)
        
        # Create a copy of the data for manipulation
        expanded_data = data.copy()
        
        # Iterate buffer_size times to progressively fill gaps
        for _ in range(buffer_size):
            # Create a mask of cells to fill (NaN cells adjacent to valid data)
            kernel = np.array([[0,1,0], [1,1,1], [0,1,0]])
            adjacent_valid = scipy.ndimage.binary_dilation(valid_mask, kernel) & ~valid_mask
            
            if not np.any(adjacent_valid):
                break
            
            # For each cell to fill, use the mean of valid adjacent cells
            y_indices, x_indices = np.where(adjacent_valid)
            for y, x in zip(y_indices, x_indices):
                # Get values of adjacent cells
                neighbors = []
                for dy, dx in [(-1,0), (1,0), (0,-1), (0,1)]:
                    ny, nx = y + dy, x + dx
                    if (0 <= ny < data.shape[0] and 
                        0 <= nx < data.shape[1] and 
                        not np.isnan(expanded_data[ny, nx])):
                        neighbors.append(expanded_data[ny, nx])
                
                if neighbors:
                    expanded_data[y, x] = np.mean(neighbors)
                    valid_mask[y, x] = True
        
        return expanded_data
