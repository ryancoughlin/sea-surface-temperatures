from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import logging
import xarray as xr
import cartopy.crs as ccrs
from .base_visualizer import BaseVisualizer
from config.settings import SOURCES
from config.regions import REGIONS
from typing import Tuple, Optional, Dict
from datetime import datetime
from matplotlib.colors import LinearSegmentedColormap

logger = logging.getLogger(__name__)

class OceanDynamicsVisualizer(BaseVisualizer):
    """Visualizer for combined ocean dynamics data (sea surface height and currents)."""
    
    def generate_image(self, data: Dict, region: str, dataset: str, date: datetime) -> Tuple[plt.Figure, Optional[Dict]]:
        """Generate visualization combining sea surface height and currents."""
        try:
            # Extract data components
            altimetry_data = data['altimetry']['data']
            currents_data = data['currents']['data']
            
            # Get coordinates
            lon_name, lat_name = self.get_coordinate_names(currents_data)
            lons = currents_data[lon_name].values
            lats = currents_data[lat_name].values
            
            # Extract variables
            if isinstance(altimetry_data, xr.Dataset):
                ssh = altimetry_data['sea_surface_height'].values
            else:
                ssh = altimetry_data.values
                
            u_current = currents_data['uo'].values
            v_current = currents_data['vo'].values
            
            # Calculate current magnitude
            current_magnitude = np.sqrt(u_current**2 + v_current**2)
            
            # Get valid data statistics for currents
            valid_magnitude = current_magnitude[~np.isnan(current_magnitude)]
            if len(valid_magnitude) == 0:
                raise ValueError("No valid current data for visualization")
            
            max_magnitude = float(np.percentile(valid_magnitude, 99))
            
            # Get valid data statistics for SSH
            valid_ssh = ssh[~np.isnan(ssh)]
            if len(valid_ssh) == 0:
                raise ValueError("No valid SSH data for visualization")
            
            # Calculate dynamic ranges for SSH using direct min/max
            ssh_vmin = float(np.min(valid_ssh))
            ssh_vmax = float(np.max(valid_ssh))
            
            # Create figure and axes
            fig, ax = self.create_axes(region)
            
            # Plot SSH as background
            ssh_mesh = ax.pcolormesh(
                lons, lats, ssh,
                transform=ccrs.PlateCarree(),
                cmap='RdBu_r',
                shading='gouraud',
                vmin=ssh_vmin,
                vmax=ssh_vmax,
                alpha=1,
                zorder=1
            )
            
        
            # Calculate arrow density
            nx, ny = len(lons), len(lats)
            skip = max(1, min(nx, ny) // 30)
            
            # Create coordinate grids for quiver plot
            lon_mesh, lat_mesh = np.meshgrid(lons[::skip], lats[::skip])
            
            # Normalize current vectors
            u_norm = u_current / max_magnitude
            v_norm = v_current / max_magnitude
            
            # Add current vectors
            quiver = ax.quiver(
                lon_mesh, lat_mesh,
                u_norm[::skip, ::skip], v_norm[::skip, ::skip],
                transform=ccrs.PlateCarree(),
                color='white',
                scale=60,
                scale_units='width',
                width=0.0012,
                headwidth=3,
                headaxislength=2.5,
                headlength=2.5,
                alpha=0.6,
                pivot='middle',
                zorder=2
            )
            
            # Add quiver key
            ax.quiverkey(
                quiver, 0.95, 0.95, 1.0, f'{max_magnitude:.1f} m/s',
                labelpos='E',
                coordinates='axes',
                fontproperties={'size': 8}
            )
            
            # Add title
            plt.title(f'Ocean Dynamics - {date.strftime("%Y-%m-%d")}', pad=10)
            
            return fig, None
            
        except Exception as e:
            logger.error(f"Error generating ocean dynamics visualization: {str(e)}")
            if isinstance(currents_data, xr.Dataset):
                logger.error(f"Currents data dimensions: {currents_data.dims}")
                logger.error(f"Currents variables: {list(currents_data.variables)}")
            raise