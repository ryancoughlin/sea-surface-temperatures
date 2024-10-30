import json
from pathlib import Path
import sys
from pathlib import Path

# Add project root to Python path
root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

from config.regions import REGIONS

def export_regions():
    """Export regions configuration to JSON file"""
    regions_list = [
        {
            "id": region_id,
            **region_data
        }
        for region_id, region_data in REGIONS.items()
    ]
    
    output_path = Path(__file__).parent.parent / "output" / "regions.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump({"regions": regions_list}, f, indent=2)

if __name__ == "__main__":
    export_regions() 