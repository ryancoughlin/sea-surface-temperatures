import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from pathlib import Path
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project root to Python path
root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

from config.regions import REGIONS

def generate_thumbnail(region_id: str, bounds: list, output_dir: Path):
    """Generate a simple thumbnail for a region"""
    logger.info(f"Generating thumbnail for {region_id}")
    
    fig = plt.figure(figsize=(3, 2), dpi=100)
    
    # Calculate center point for projection
    lon_min, lat_min = bounds[0]
    lon_max, lat_max = bounds[1]
    center_lon = (lon_min + lon_max) / 2
    
    ax = plt.axes(projection=ccrs.Mercator(
        central_longitude=center_lon,
        min_latitude=lat_min,
        max_latitude=lat_max
    ))
    
    # Set bounds
    ax.set_extent([lon_min, lon_max, lat_min, lat_max])
    
    # Add simple features
    ax.add_feature(cfeature.LAND, facecolor='#E0E0E0')
    ax.add_feature(cfeature.OCEAN, facecolor='white')
    
    # Remove spines and ticks for clean look
    ax.spines['geo'].set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])
    
    # Save thumbnail
    output_path = output_dir / f"{region_id}_thumb.png"
    plt.savefig(output_path, bbox_inches='tight', pad_inches=0, 
                transparent=True, dpi=100)
    plt.close()
    
    logger.info(f"Saved thumbnail to {output_path}")
    return output_path.name

def update_regions_file(thumbnails_dict):
    """Update regions.py file with thumbnail paths"""
    logger.info("Updating regions.py file")
    regions_file = root_dir / "config" / "regions.py"
    
    with open(regions_file, 'r') as f:
        content = f.read()
    
    # Find the REGIONS dictionary definition
    start = content.find("REGIONS")
    if start == -1:
        raise ValueError("Could not find REGIONS dictionary in regions.py")
    
    # Update TypedDict to include thumbnail
    if "thumbnail: str" not in content:
        logger.info("Adding thumbnail field to Region TypedDict")
        type_def = content.find("class Region(TypedDict):")
        if type_def != -1:
            insert_pos = content.find("\n", type_def) + 1
            content = (
                content[:insert_pos] +
                "    thumbnail: str\n" +
                content[insert_pos:]
            )
    
    # Add thumbnail paths to each region
    for region_id, thumb_path in thumbnails_dict.items():
        logger.info(f"Adding thumbnail path for {region_id}")
        region_start = content.find(f'"{region_id}": {{')
        if region_start == -1:
            logger.warning(f"Could not find region {region_id} in regions.py")
            continue
        
        # Find the end of this region's dict
        region_end = content.find("}", region_start)
        region_content = content[region_start:region_end]
        
        # Add thumbnail if not present
        if '"thumbnail":' not in region_content:
            content = (
                content[:region_end] +
                f',\n        "thumbnail": "/assets/region_thumbnails/{thumb_path}"' +
                content[region_end:]
            )
    
    with open(regions_file, 'w') as f:
        f.write(content)
    logger.info("Successfully updated regions.py")

def generate_all_thumbnails():
    """Generate thumbnails for all regions and update regions.py"""
    logger.info("Starting thumbnail generation")
    output_dir = root_dir / "assets" / "region_thumbnails"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    thumbnails = {}
    for region_id, region_data in REGIONS.items():
        thumb_name = generate_thumbnail(region_id, region_data["bounds"], output_dir)
        thumbnails[region_id] = thumb_name
    
    update_regions_file(thumbnails)
    logger.info("Completed thumbnail generation and file updates")

if __name__ == "__main__":
    generate_all_thumbnails() 