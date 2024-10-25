from pathlib import Path
from datetime import datetime
from typing import Dict
import json
import logging
from config.settings import SOURCES, OUTPUT_DIR
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class MetadataAssembler:
    def __init__(self):
        self.output_dir = OUTPUT_DIR
    
    def assemble(self, 
                region: str,
                dataset: str,
                timestamp: str,
                data_path: Path,
                image_path: Path,
                geojson_path: Path) -> Path:
        """Assemble metadata for processed data."""
        try:
            metadata = {
                "dataset": {
                    "id": dataset,
                    "name": SOURCES[dataset].get('name', dataset),
                    "category": SOURCES[dataset].get('category', 'unknown'),
                    "variable": SOURCES[dataset].get('variable', []),
                },
                "region": {
                    "id": region,
                    "name": REGIONS[region].get('name', region),
                    "bounds": REGIONS[region]['bounds'],
                },
                "timestamp": timestamp,
                "processing_time": datetime.utcnow().isoformat(),
                "files": {
                    "image": str(image_path),
                    "geojson": str(geojson_path),
                    "source": str(data_path)
                }
            }
            
            # Save metadata
            output_path = self.output_dir / "metadata" / region / dataset / f"{timestamp}.json"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Generated metadata file: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error assembling metadata: {str(e)}")
            raise