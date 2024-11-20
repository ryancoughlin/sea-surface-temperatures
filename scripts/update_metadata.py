import json
from pathlib import Path
from datetime import datetime

SERVER_URL = 'http://157.245.10.94'

def process_path(path):
    """Convert local path to full URL."""
    return f"{SERVER_URL}/{str(path).replace('output/', '')}"

def update_metadata():
    metadata_path = Path('output/metadata.json')
    if not metadata_path.exists():
        print("❌ metadata.json not found")
        return
        
    try:
        # Read current metadata
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
            
        # Process all layer paths in the nested structure
        for region in metadata.get("regions", []):
            for dataset in region.get("datasets", []):
                for date_entry in dataset.get("dates", []):
                    layers = date_entry.get("layers", {})
                    # Update each layer path
                    for layer_type, path in layers.items():
                        if isinstance(path, str):
                            layers[layer_type] = process_path(path)
        
        # Write back updated metadata
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        print("✅ Successfully updated metadata.json")
        
    except Exception as e:
        print(f"❌ Error updating metadata: {str(e)}")

if __name__ == "__main__":
    update_metadata() 