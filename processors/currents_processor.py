from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import logging
from .base_processor import BaseImageProcessor
from config.settings import SOURCES
from config.regions import REGIONS
import xarray as xr
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class CurrentsProcessor(BaseImageProcessor):
    def generate_image(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Generate ocean currents visualization with readable arrow coverage."""
        try:
            # Load data
            ds = xr.open_dataset(data_path)
            bounds = REGIONS[region]['bounds']
            
            # Subset data
            ds_subset = ds.sel(
                longitude=slice(bounds[0][0], bounds[1][0]),
                latitude=slice(bounds[0][1], bounds[1][1]),
                time=ds.time[0]
            )
            
            # Get current components
            u = ds_subset.u_current
            v = ds_subset.v_current
            
            # Create figure
            fig, ax = plt.subplots(figsize=(10, 8), facecolor='none')
            ax.set_facecolor('none')
            
            # Create grid
            lon_grid, lat_grid = np.meshgrid(ds_subset.longitude, ds_subset.latitude)
            
            # Add stride to reduce density
            stride = 4  # Show every 4th point
            
            # Plot arrows
            ax.quiver(
                lon_grid[::stride, ::stride],  # Reduced density
                lat_grid[::stride, ::stride],
                u.values[::stride, ::stride].T,
                v.values[::stride, ::stride].T,
                color='white',
                scale=8,  # Adjusted for more visible arrows
                width=0.004,  # Thicker arrows
                headwidth=6,  # Larger arrow heads
                headlength=7,
                headaxislength=6,
                alpha=0.8  # More opaque
            )
            
            # Clean up plot
            ax.axis('off')
            plt.tight_layout(pad=0)
            
            # Save image
            image_path = self.generate_image_path(region, dataset, timestamp)
            image_path.parent.mkdir(parents=True, exist_ok=True)
            
            fig.savefig(
                image_path,
                dpi=300,
                bbox_inches='tight',
                transparent=True,
                pad_inches=0
            )
            plt.close(fig)
            
            logger.info(f"Currents image saved to {image_path}")
            return image_path
            
        except Exception as e:
            logger.error(f"Error processing currents data: {str(e)}")
            raise

    def process_current_data(self, ds, bounds):
        """Process current data into useful formats."""
        # 1. Extract our region
        region_mask = (
            (ds.lon >= bounds[0][0]) & (ds.lon <= bounds[1][0]) &
            (ds.lat >= bounds[0][1]) & (ds.lat <= bounds[1][1])
        )
        
        # 2. Calculate derived values
        speed = np.sqrt(ds.u**2 + ds.v**2)  # magnitude of current
        direction = np.arctan2(ds.v, ds.u)   # direction in radians
        
        # 3. Create a clean data structure
        currents_data = {
            'lon': ds.lon.where(region_mask, drop=True),
            'lat': ds.lat.where(region_mask, drop=True),
            'u': ds.u.where(region_mask, drop=True),
            'v': ds.v.where(region_mask, drop=True),
            'speed': speed.where(region_mask, drop=True),
            'direction': direction.where(region_mask, drop=True)
        }
        
        # Log what we got
        logger.info(f"Processed current data:")
        logger.info(f"Region shape: {currents_data['lon'].shape}")
        logger.info(f"Speed range: {currents_data['speed'].min().item():.2f} to {currents_data['speed'].max().item():.2f} m/s")
        
        return currents_data

    def plot_currents(self, data, ax):
        """Plot currents using processed data."""
        # Create grid for plotting
        lon, lat = np.meshgrid(data['lon'], data['lat'])
        
        # Plot speed as background color
        speed = data['speed'].squeeze().T
        contour = ax.contourf(
            lon, lat, speed,
            levels=np.linspace(0, speed.max().item(), 21),
            cmap='viridis',
            extend='both'
        )
        
        # Add arrows - subsample for clarity
        stride = max(1, min(speed.shape) // 20)  # adjust based on zoom level
        ax.quiver(
            lon[::stride, ::stride],
            lat[::stride, ::stride],
            data['u'].squeeze().values[::stride, ::stride].T,
            data['v'].squeeze().values[::stride, ::stride].T,
            scale=20,
            width=0.003,
            color='white',
            alpha=0.6
        )
        
        return contour

    def _debug_to_geojson(self, ds, bounds):
        """Temporary debug function to convert currents data to GeoJSON."""
        try:
            # Get coordinate names
            lon_name = 'longitude' if 'longitude' in ds.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in ds.coords else 'lat'
            
            # Create masks for the region
            lon_mask = (ds[lon_name] >= bounds[0][0]) & (ds[lon_name] <= bounds[1][0])
            lat_mask = (ds[lat_name] >= bounds[0][1]) & (ds[lat_name] <= bounds[1][1])
            
            # Get the points within bounds
            lons = ds[lon_name].where(lon_mask & lat_mask, drop=True)
            lats = ds[lat_name].where(lon_mask & lat_mask, drop=True)
            
            # Get u and v components for first time step
            u = ds.u.isel(time=0)
            v = ds.v.isel(time=0)
            
            # Create GeoJSON structure
            features = []
            
            # Log the shapes
            logger.info(f"Debug - Coordinate shapes:")
            logger.info(f"Longitudes: {lons.shape}")
            logger.info(f"Latitudes: {lats.shape}")
            logger.info(f"U values shape: {u.shape}")
            logger.info(f"V values shape: {v.shape}")
            
            # Sample points (to keep output manageable)
            stride = max(1, min(u.shape) // 20)
            
            for i in range(0, len(lons), stride):
                for j in range(0, len(lats), stride):
                    try:
                        lon = float(lons[i].item())
                        lat = float(lats[j].item())
                        u_val = float(u[i, j].item())
                        v_val = float(v[i, j].item())
                        speed = float(np.sqrt(u_val**2 + v_val**2))
                        
                        feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [lon, lat]
                            },
                            "properties": {
                                "u": u_val,
                                "v": v_val,
                                "speed": speed
                            }
                        }
                        features.append(feature)
                    except Exception as e:
                        logger.debug(f"Skipping point ({i},{j}): {str(e)}")
            
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "timestamp": datetime.now().isoformat(),
                    "point_count": len(features),
                    "bounds": bounds
                }
            }
            
            # Save to file for inspection
            debug_file = Path("debug_currents.geojson")
            with open(debug_file, 'w') as f:
                json.dump(geojson, f, indent=2)
            
            logger.info(f"Debug GeoJSON saved to {debug_file}")
            logger.info(f"Total points: {len(features)}")
            
            return geojson
            
        except Exception as e:
            logger.error(f"Error creating debug GeoJSON: {str(e)}")
            logger.error("Full traceback:", exc_info=True)
