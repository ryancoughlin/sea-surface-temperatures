import json
import sys
import logging
import os
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to Python path
ROOT_DIR = Path(__file__).parent.parent
sys.path.append(str(ROOT_DIR))

from config.settings import PATHS, SERVER_URL
from config.regions import REGIONS

# Thumbnail configuration
THUMBNAIL_CONFIG = {
    'figsize': (4, 3),  # 4:3 ratio
    'dpi': 300,
    'land_color': '#EBE59B',
    'ocean_color': '#B1C2D8',
    'extent_buffer': 0.1,  # 10% buffer around region bounds
    'scale': '50m'  # Cartopy feature scale
}

def ensure_directory(path: Path) -> None:
    """Create directory if it doesn't exist and set permissions.
    
    Args:
        path: Directory path to create
    """
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, 0o755)  # rwxr-xr-x
    except Exception as e:
        logger.warning(f"Could not set permissions for {path}: {e}")

def generate_thumbnail(region_id: str, bounds: List[Tuple[float, float]], output_dir: Path) -> str:
    """Generate a thumbnail for a region.
    
    Args:
        region_id: Identifier for the region
        bounds: List of [lon, lat] pairs defining region bounds
        output_dir: Directory to save thumbnail
        
    Returns:
        str: Name of generated thumbnail file
    """
    logger.info(f"Generating thumbnail for {region_id}")
    
    # Extract bounds
    lon_min, lat_min = bounds[0]  # Southwest corner
    lon_max, lat_max = bounds[1]  # Northeast corner
    
    # Calculate center and add buffer
    center_lon = (lon_min + lon_max) / 2
    center_lat = (lat_min + lat_max) / 2
    buffer = max(lon_max - lon_min, lat_max - lat_min) * THUMBNAIL_CONFIG['extent_buffer']
    
    # Create buffered extent
    extent = [
        lon_min - buffer,
        lon_max + buffer,
        lat_min - buffer,
        lat_max + buffer
    ]
    
    # Set up figure
    fig = plt.figure(figsize=THUMBNAIL_CONFIG['figsize'], dpi=THUMBNAIL_CONFIG['dpi'])
    
    # Configure projection
    projection = ccrs.Mercator(
        central_longitude=center_lon,
        min_latitude=extent[2],
        max_latitude=extent[3]
    )
    
    # Set up axes and features
    ax = plt.axes(projection=projection)
    ax.set_extent(extent, crs=ccrs.PlateCarree())
    
    # Add land and ocean features
    for feature, color in [(cfeature.LAND, THUMBNAIL_CONFIG['land_color']),
                          (cfeature.OCEAN, THUMBNAIL_CONFIG['ocean_color'])]:
        ax.add_feature(
            feature.with_scale(THUMBNAIL_CONFIG['scale']),
            facecolor=color
        )
    
    # Clean up appearance
    ax.spines['geo'].set_visible(False)
    ax.set_facecolor('none')
    ax.set_xticks([])
    ax.set_yticks([])
    
    # Save thumbnail
    output_path = output_dir / f"{region_id}.png"
    plt.savefig(
        output_path,
        bbox_inches='tight',
        pad_inches=0,
        transparent=True,
        dpi=THUMBNAIL_CONFIG['dpi'],
        format='png'
    )
    plt.close(fig)
    
    # Set file permissions
    try:
        os.chmod(output_path, 0o644)  # rw-r--r--
    except Exception as e:
        logger.warning(f"Could not set permissions for {output_path}: {e}")
    
    return output_path.name

def generate_region_groups() -> List[Dict]:
    """Generate grouped region data with thumbnails.
    
    Returns:
        List[Dict]: List of region groups with their regions
    """
    # Ensure thumbnails directory exists
    thumbnails_dir = PATHS['STATIC_DIR'] / 'region_thumbnails'
    ensure_directory(thumbnails_dir)
    
    # Create regions list with thumbnails
    regions_list = []
    for region_id, region_data in REGIONS.items():
        # Generate thumbnail
        thumbnail_name = generate_thumbnail(region_id, region_data["bounds"], thumbnails_dir)
        
        # Create region entry
        regions_list.append({
            "id": region_id,
            "thumbnail": f"{SERVER_URL}/static/region_thumbnails/{thumbnail_name}",
            **region_data
        })
    
    # Sort and group regions
    regions_list.sort(key=lambda x: (x['group'], x['name']))
    
    # Group by region group
    groups = {}
    for region in regions_list:
        group_name = region['group']
        if group_name not in groups:
            groups[group_name] = []
        groups[group_name].append(region)
    
    # Convert to sorted list of groups
    return [
        {"group": name, "regions": regions}
        for name, regions in sorted(groups.items())
    ]

def export_regions() -> None:
    """Export region configuration with thumbnails to JSON."""
    try:
        logger.info("Starting region export")
        
        # Generate region groups with thumbnails
        grouped_regions = generate_region_groups()
        
        # Prepare output
        output = {
            "groups": grouped_regions,
            "server_url": SERVER_URL,
            "generated_at": datetime.now().isoformat()
        }
        
        # Save to JSON
        output_path = PATHS['API_DIR'] / 'regions.json'
        ensure_directory(output_path.parent)
        
        with open(output_path, 'w') as f:
            json.dump(output, f, indent=2)
            
        # Set file permissions
        os.chmod(output_path, 0o644)  # rw-r--r--
        
        logger.info(f"Successfully exported regions to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to export regions: {e}")
        raise

if __name__ == "__main__":
    export_regions() 