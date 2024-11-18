import json
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
from scripts.generate_thumbnails import generate_all_thumbnails

def export_regions():
    """Export regions configuration to JSON file"""
    logger.info("Starting regions export")
    
    # Generate thumbnails first and get the mapping of region_id to thumbnail names
    logger.info("Generating thumbnails")
    thumbnail_mapping = generate_all_thumbnails()
    
    # Reload REGIONS to get updated data
    from importlib import reload
    import config.regions
    reload(config.regions)
    from config.regions import REGIONS
    
    logger.info("Creating regions list")
    regions_list = [
        {
            "id": region_id,
            "thumbnail": f"/assets/region_thumbnails/{thumbnail_mapping[region_id]}",
            **region_data
        }
        for region_id, region_data in REGIONS.items()
    ]
    
    output_path = root_dir / "output" / "regions.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Writing regions to {output_path}")
    with open(output_path, 'w') as f:
        json.dump({"regions": regions_list}, f, indent=2)
    
    logger.info("Export complete")

if __name__ == "__main__":
    export_regions() 