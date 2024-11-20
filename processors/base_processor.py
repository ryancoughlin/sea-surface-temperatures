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
        """Save figure with optimized PNG compression."""
        try:
            path = self.path_manager.get_asset_paths(date, dataset, region)
            
            # First save to BytesIO to avoid writing to disk twice
            buf = BytesIO()
            fig.savefig(
                buf,
                dpi=self.settings['dpi'],
                bbox_inches=None,
                pad_inches=0,
                transparent=True,
                format='png'
            )
            plt.close(fig)
            
            # Get original size
            original_size = len(buf.getvalue())
            logger.info(f"Original size: {original_size/1024:.1f}KB")
            
            # Open with Pillow
            img = Image.open(buf)
            
            # Test each optimization method
            
            # 1. PIL's built-in optimization
            pil_buf = BytesIO()
            img.save(
                pil_buf,
                'PNG',
                optimize=True,
                quality=85
            )
            pil_size = len(pil_buf.getvalue())
            logger.info(f"PIL optimized size: {pil_size/1024:.1f}KB ({(pil_size/original_size)*100:.1f}%)")
            
            # Save the PIL-optimized version as our default
            img.save(
                path.image,
                'PNG',
                optimize=True,
                quality=85
            )
            
            # 2. OptiPNG (if installed)
            try:
                subprocess.run(['optipng', '-o2', str(path.image)], check=True)
                optipng_size = path.image.stat().st_size
                logger.info(f"OptiPNG size: {optipng_size/1024:.1f}KB ({(optipng_size/original_size)*100:.1f}%)")
            except (subprocess.SubprocessError, FileNotFoundError):
                logger.warning("OptiPNG not available")
                
            # 3. pngquant (if installed)
            try:
                temp_path = path.image.with_suffix('.temp.png')
                path.image.rename(temp_path)
                subprocess.run(['pngquant', '--force', '--output', str(path.image), str(temp_path)], check=True)
                temp_path.unlink()  # Clean up temp file
                pngquant_size = path.image.stat().st_size
                logger.info(f"pngquant size: {pngquant_size/1024:.1f}KB ({(pngquant_size/original_size)*100:.1f}%)")
            except (subprocess.SubprocessError, FileNotFoundError):
                logger.warning("pngquant not available")
                
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

    def setup_map(self, ax, bounds):
        """Setup basic map features."""
        ax.set_extent([bounds[0][0], bounds[1][0], bounds[0][1], bounds[1][1]], crs=ccrs.PlateCarree())
        ax.coastlines(resolution='10m')
        ax.gridlines(draw_labels=True)

    def save_image(self, fig, region: str, dataset: str, date: str) -> Path:
        """Save the figure to the appropriate path."""
        if not self.path_manager:
            raise ValueError("PathManager not initialized")
            
        # Get the image path 
        image_path = self.path_manager.get_asset_paths(date, dataset, region).image
        
        # Ensure directory exists
        image_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save the figure
        fig.savefig(image_path, bbox_inches='tight', dpi=300)
        plt.close(fig)
        
        return image_path
