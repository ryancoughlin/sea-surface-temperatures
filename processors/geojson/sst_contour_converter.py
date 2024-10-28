import numpy as np
import xarray as xr
from pathlib import Path
import json
import logging
import cartopy.crs as ccrs
from matplotlib import pyplot as plt
from .base_converter import BaseGeoJSONConverter
from utils.data_utils import convert_temperature_to_f
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class SSTContourConverter(BaseGeoJSONConverter):
    def convert(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Convert SST data to contour GeoJSON format."""
        try:
            # Load and prepare data
            ds = self.load_dataset(data_path)
            data = self.select_time_slice(ds['sea_surface_temperature'])
            
            # Get coordinate names and region bounds
            lon_name = 'longitude' if 'longitude' in data.coords else 'lon'
            lat_name = 'latitude' if 'latitude' in data.coords else 'lat'
            bounds = REGIONS[region]['bounds']
            
            # Mask to region
            lon_mask = (data[lon_name] >= bounds[0][0]) & (data[lon_name] <= bounds[1][0])
            lat_mask = (data[lat_name] >= bounds[0][1]) & (data[lat_name] <= bounds[1][1])
            regional_data = data.where(lon_mask & lat_mask, drop=True)
            
            # Convert to Fahrenheit
            regional_data = convert_temperature_to_f(regional_data)
            
            # Create figure with PlateCarree projection for geographic coordinates
            fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree()})
            
            # Set the map extent in geographic coordinates
            ax.set_extent([
                bounds[0][0], bounds[1][0],  # lon bounds
                bounds[0][1], bounds[1][1]   # lat bounds
            ], crs=ccrs.PlateCarree())
            
            # Generate contours in geographic coordinates
            contour_lines = ax.contour(
                regional_data[lon_name],
                regional_data[lat_name],
                regional_data,
                levels=np.arange(32, 89, 2),  # Temperature contours every 2°F
                transform=ccrs.PlateCarree()
            )
            
            # Convert to GeoJSON (coordinates will be in lon/lat)
            contour_geojson = {
                "type": "FeatureCollection",
                "features": []
            }
            
            for i, collection in enumerate(contour_lines.collections):
                for path in collection.get_paths():
                    # Get vertices in data coordinates (lon/lat)
                    vertices = path.vertices
                    
                    if len(vertices) > 1:  # Only add if we have a valid line
                        feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "LineString",
                                "coordinates": vertices.tolist()
                            },
                            "properties": {
                                "temperature": float(contour_lines.levels[i]),
                                "label": f"{contour_lines.levels[i]}°F"
                            }
                        }
                        contour_geojson["features"].append(feature)
            
            # Cleanup
            plt.close(fig)
            
            # Save to file
            output_path = self.base_dir / region / "datasets" / dataset / timestamp / "contours.geojson"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(contour_geojson, f)
                
            return output_path
            
        except Exception as e:
            logger.error(f"Error converting SST contours to GeoJSON: {str(e)}")
            raise
