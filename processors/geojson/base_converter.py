from abc import ABC, abstractmethod
from pathlib import Path
import logging
from config.settings import REGIONS_DIR

logger = logging.getLogger(__name__)

class BaseGeoJSONConverter(ABC):
    """Base class for GeoJSON converters."""
    
    def __init__(self):
        self.base_dir = REGIONS_DIR
    
    @abstractmethod
    def convert(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Convert data to GeoJSON format."""
        pass
    
    def generate_geojson_path(self, region: str, dataset: str, timestamp: str) -> Path:
        """Generate standard output path for GeoJSON files."""
        path = self.base_dir / region / "datasets" / dataset / timestamp / "data.geojson"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
