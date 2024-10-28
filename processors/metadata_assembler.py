from pathlib import Path
from datetime import datetime
import logging
import json
from config.settings import SOURCES, REGIONS_DIR
from config.regions import REGIONS
from .output_manager import OutputManager, DatasetMetadata
from typing import Optional, Dict

logger = logging.getLogger(__name__)

class MetadataAssembler:
    def __init__(self):
        self.output_manager = OutputManager(REGIONS_DIR)

    def assemble_metadata(
        self,
        region: str,
        dataset: str,
        timestamp: str,
        image_path: Path,
        geojson_path: Path,
        additional_layers: Optional[Dict] = None
    ) -> Path:
        """Assemble metadata for any dataset type."""
        try:
            dataset_config = SOURCES[dataset]
            
            metadata = {
                "dataset": {
                    "id": dataset,
                    "name": dataset_config['name'],
                    "category": dataset_config['category']
                },
                "timestamp": timestamp,
                "paths": {
                    "image": str(image_path.relative_to(REGIONS_DIR)),
                    "data": str(geojson_path.relative_to(REGIONS_DIR))
                },
                "layers": {
                    "base": {
                        "type": dataset_config['category'],
                        "source": "image"
                    }
                }
            }

            # Add additional layers if present
            if additional_layers:
                metadata["paths"].update(
                    {name: layer["path"] for name, layer in additional_layers.items()}
                )
                metadata["layers"].update(
                    {name: {k:v for k,v in layer.items() if k != "path"}
                     for name, layer in additional_layers.items()}
                )

            output_dir = REGIONS_DIR / region / "datasets" / dataset / timestamp
            output_dir.mkdir(parents=True, exist_ok=True)
            metadata_path = output_dir / "data.json"
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)

            return metadata_path

        except Exception as e:
            logger.error(f"Error assembling metadata: {str(e)}")
            raise
