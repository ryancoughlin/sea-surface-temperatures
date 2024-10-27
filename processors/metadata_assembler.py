from pathlib import Path
from datetime import datetime
import json
import logging
from config.settings import SOURCES, REGIONS_DIR
from config.regions import REGIONS
from .output_manager import OutputManager, DatasetMetadata

logger = logging.getLogger(__name__)

class MetadataAssembler:
    def __init__(self):
        self.output_manager = OutputManager(REGIONS_DIR)

    def assemble_metadata(self, region: str, dataset: str, timestamp: str,
                         image_path: Path, geojson_path: Path, mapbox_url: str = None) -> Path:
        """Assemble and save metadata for a processed dataset."""
        try:
            source_info = self.output_manager.get_source_config(dataset)
            
            metadata = DatasetMetadata(
                id=dataset,
                name=source_info.get('name', dataset),
                category=source_info.get('category', 'unknown'),
                timestamp=timestamp,
                image_path=image_path,
                geojson_path=geojson_path,
                mapbox_url=mapbox_url
            )

            return self.output_manager.save_dataset(metadata, region)

        except Exception as e:
            logger.error(f"Error assembling metadata: {str(e)}")
            raise
