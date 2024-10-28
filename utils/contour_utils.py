from typing import Tuple, Dict, Any
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.contour import QuadContourSet
import cartopy.crs as ccrs
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)

def generate_temperature_contours(
    ax: plt.Axes,
    data: np.ndarray,
    lons: np.ndarray,
    lats: np.ndarray,
    temp_interval: int = 5,
    min_temp: int = 35,
    max_temp: int = 85
) -> QuadContourSet:
    """Generate temperature contour lines with labels."""
    temp_levels = np.arange(min_temp, max_temp, temp_interval)
    
    contour_lines = ax.contour(
        lons,
        lats,
        data,
        levels=temp_levels,
        colors='black',
        linewidths=0.5,
        transform=ccrs.PlateCarree()
    )
    
    ax.clabel(contour_lines, contour_lines.levels, inline=True, fmt='%d°F', fontsize=8)
    
    return contour_lines

def contours_to_geojson(
    contour_lines: QuadContourSet
) -> Dict[str, Any]:
    """Convert matplotlib contour lines to GeoJSON format."""
    contour_geojson = {
        "type": "FeatureCollection",
        "features": [],
    }
    
    for i, collection in enumerate(contour_lines.collections):
        for path in collection.get_paths():
            vertices = path.vertices
            coordinates = vertices.tolist()
            
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": coordinates
                },
                "properties": {
                    "temperature": float(contour_lines.levels[i]),
                    "label": f"{int(contour_lines.levels[i])}°F"
                }
            }
            contour_geojson["features"].append(feature)
    
    return contour_geojson
