from pathlib import Path
from datetime import datetime
import logging
import json
from config.settings import SOURCES, REGIONS_DIR, OUTPUT_DIR
from config.regions import REGIONS
from typing import Optional, Dict

logger = logging.getLogger(__name__)

class MetadataAssembler:
    def __init__(self):
        pass

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
            "dates": [
                {
                    "date": timestamp,
                    "layers": {
                        "image": str(image_path),
                        "geojson": str(geojson_path),
                    }
                }
            ],
            "supportedLayers": SOURCES[dataset].get("supportedLayers", []),
            "type": SOURCES[dataset].get("type", ""),
        }

        if additional_layers:
            if "contours" in additional_layers:
                metadata["contours"] = str(additional_layers["contours"]["layers"])

        dataset_dir = Path(REGIONS_DIR) / region / "datasets" / dataset
        dataset_dir.mkdir(parents=True, exist_ok=True)

        metadata_path = dataset_dir / "metadata.json"
        logger.info(f"Saving metadata to {metadata_path}")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f)

        return metadata_path
