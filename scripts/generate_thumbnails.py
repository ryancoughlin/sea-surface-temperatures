import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from pathlib import Path
import sys
import logging
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project root to Python path
root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

from config.regions import REGIONS

# Define consistent styling
THUMBNAIL_STYLE = {
    'figsize': (4, 3),  # 4:3 ratio
    'dpi': 100,
    'land_color': '#ECE4C9',  # Light gray for land
    'ocean_color': '#AAC3E4',   # White for ocean
    'extent_buffer': 0.1      # 10% buffer around region bounds
}

def generate_thumbnail(region_id: str, bounds: list, output_dir: Path):
    """Generate a simple thumbnail for a region with consistent 4:3 ratio"""
    logger.info(f"Generating thumbnail for {region_id}")
    
    # Calculate bounds with buffer
    lon_min, lat_min = bounds[0]
    lon_max, lat_max = bounds[1]
    
    # Calculate center and span
    center_lon = (lon_min + lon_max) / 2
    center_lat = (lat_min + lat_max) / 2
    lon_span = lon_max - lon_min
    lat_span = lat_max - lat_min
    
    # Add buffer to maintain aspect ratio
    buffer = max(lon_span, lat_span) * THUMBNAIL_STYLE['extent_buffer']
    lon_min -= buffer
    lon_max += buffer
    lat_min -= buffer
    lat_max += buffer
    
    # Create figure with exact 4:3 ratio
    fig = plt.figure(figsize=THUMBNAIL_STYLE['figsize'], dpi=THUMBNAIL_STYLE['dpi'])
    
    # Set up projection
    projection = ccrs.Mercator(
        central_longitude=center_lon,
        min_latitude=lat_min,
        max_latitude=lat_max
    )
    
    ax = plt.axes(projection=projection)
    ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=ccrs.PlateCarree())
    
    # Add features with consistent styling
    land = cfeature.LAND.with_scale('50m')  # Use medium resolution for better performance
    ocean = cfeature.OCEAN.with_scale('50m')
    
    ax.add_feature(land, facecolor=THUMBNAIL_STYLE['land_color'])
    ax.add_feature(ocean, facecolor=THUMBNAIL_STYLE['ocean_color'])
    
    # Remove all decorations using modern API
    ax.spines['geo'].set_visible(False)  # Replaces outline_patch
    ax.set_facecolor('none')  # Replaces background_patch.set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])
    
    # Save with consistent settings
    output_path = output_dir / f"{region_id}.png"
    plt.savefig(output_path,
                bbox_inches='tight',
                pad_inches=0,
                transparent=True,
                dpi=THUMBNAIL_STYLE['dpi'],
                format='png')
    plt.close(fig)
    
    logger.info(f"Saved thumbnail to {output_path}")
    return output_path.name

def generate_all_thumbnails():
    """Generate thumbnails for all regions"""
    logger.info("Starting thumbnail generation")
    output_dir = root_dir / "assets" / "region_thumbnails"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    thumbnails = {}
    for region_id, region_data in REGIONS.items():
        thumb_name = generate_thumbnail(region_id, region_data["bounds"], output_dir)
        thumbnails[region_id] = thumb_name
    
    logger.info("Completed thumbnail generation")
    return thumbnails

if __name__ == "__main__":
    generate_all_thumbnails() 