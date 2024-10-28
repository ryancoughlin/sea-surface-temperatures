from pathlib import Path
from datetime import datetime
import logging
import json
from config.settings import SOURCES, REGIONS_DIR, OUTPUT_DIR
from config.regions import REGIONS
from .output_manager import OutputManager
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
        metadata = {
            "id": dataset,
            "name": SOURCES[dataset]['name'],
            "category": SOURCES[dataset]['category'],
            "dates": [
                {
                    "date": timestamp,
                    "layers": {
                        "image": str(image_path),
                        "geojson": str(geojson_path),
                    }
                }
            ],
        }

        if additional_layers and "contours" in additional_layers:
            metadata["dates"][0]["layers"]["contours"] = str(additional_layers["contours"])

        dataset_dir = Path(REGIONS_DIR) / region / "datasets" / dataset / timestamp
        dataset_dir.mkdir(parents=True, exist_ok=True)

        metadata_path = dataset_dir / "metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f)

        return metadata_path
