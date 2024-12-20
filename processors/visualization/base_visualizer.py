import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple, Any
from config.settings import IMAGE_SETTINGS
from config.regions import REGIONS
from utils.path_manager import PathManager
from PIL import Image
from io import BytesIO
from scipy.ndimage import binary_dilation, distance_transform_edt
import logging

logger = logging.getLogger(__name__)

class BaseVisualizer(ABC):
    """Base class for data visualization."""
    
    def __init__(self, path_manager: PathManager):
        self.path_manager = path_manager
        self.settings = IMAGE_SETTINGS
        
    @abstractmethod
    def generate_image(self, data: xr.Dataset, region: str, dataset: str, date: str) -> Tuple[plt.Figure, Optional[Dict]]:
        """Generate visualization from data.
        
        Args:
            data: Input dataset
            region: Region identifier
            dataset: Source dataset identifier
            date: Date string for the visualization
            
        Returns:
            tuple: (matplotlib figure, optional metadata dictionary)
        """
        pass
        
    def save_image(self, data: xr.Dataset, region: str, dataset: str, date: datetime, asset_paths: Optional[Any] = None) -> Path:
        """Save visualization to file with optimization.
        
        Args:
            data: Input dataset
            region: Region identifier
            dataset: Dataset identifier
            date: Processing date
            asset_paths: Optional asset paths object. If not provided, will be fetched from path manager.
            
        Returns:
            Path to saved image file
        """
        try:
            # Generate the visualization
            fig, metadata = self.generate_image(data, region, dataset, date)
            
            if fig is None:
                raise ValueError("No figure generated")
                
            # Get paths
            if asset_paths is None:
                output_path = self.path_manager.get_asset_paths(date, dataset, region).image
            else:
                output_path = asset_paths.image
                
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save to BytesIO first for optimization
            buf = BytesIO()
            fig.savefig(
                buf,
                dpi=self.settings['dpi'],
                bbox_inches='tight',
                pad_inches=0,
                transparent=True,
                format='png'
            )
            plt.close(fig)
            
            # Write optimized buffer to file
            buf.seek(0)
            with open(output_path, 'wb') as f:
                f.write(buf.getvalue())
            
            return output_path
            
        except Exception as e:
            logger.error(f"Error saving visualization: {str(e)}")
            raise
            
    def create_axes(self, region: str) -> Tuple[plt.Figure, plt.Axes]:
        """Create figure and map projection axes."""
        try:
            bounds = REGIONS[region]['bounds']
            
            # Calculate aspect ratio from bounds
            lon_span = bounds[1][0] - bounds[0][0]
            lat_span = bounds[1][1] - bounds[0][1]
            aspect = lon_span / lat_span
            
            # Create figure with exact size ratio
            height = self.settings.get('height', 24)
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
                bounds[0][0],  # min lon
                bounds[1][0],  # max lon
                bounds[0][1],  # min lat
                bounds[1][1]   # max lat
            ], crs=ccrs.PlateCarree())
            
            return fig, ax
            
        except Exception as e:
            logger.error(f"Error creating axes: {str(e)}")
            raise
            
    def get_coordinate_names(self, data: xr.Dataset) -> tuple:
        """Get standardized coordinate names."""
        lon_patterns = ['lon', 'longitude', 'x']
        lat_patterns = ['lat', 'latitude', 'y']
        
        lon_name = None
        lat_name = None
        
        for var in data.coords:
            var_lower = var.lower()
            if any(pattern in var_lower for pattern in lon_patterns):
                lon_name = var
            elif any(pattern in var_lower for pattern in lat_patterns):
                lat_name = var
                
        if not lon_name or not lat_name:
            raise ValueError("Could not identify coordinate variables")
            
        return lon_name, lat_name
            
    def expand_coastal_data(self, data: xr.Dataset) -> xr.Dataset:
        """Expand coastal data points to improve visualization."""
        kernel = np.array([[0, 1, 0], [1, 1, 1], [0, 1, 0]])
        expanded_data = data.copy()
        
        for var in data.data_vars:
            values = data[var].values
            mask = ~np.isnan(values)
            
            # Apply dilation
            expanded_mask = binary_dilation(mask, structure=kernel, iterations=2)
            
            # Fill expanded areas with nearest valid value
            if mask.any():
                dist, indices = distance_transform_edt(
                    ~mask, 
                    return_indices=True
                )
                
                expanded_values = values.copy()
                expanded_values[expanded_mask] = values[
                    indices[0][expanded_mask],
                    indices[1][expanded_mask]
                ]
                
                # Update only the expanded areas
                values[expanded_mask & ~mask] = expanded_values[expanded_mask & ~mask]
                expanded_data[var].values = values
                
        return expanded_data