from abc import ABC, abstractmethod
from pathlib import Path
import xarray as xr
import logging

logger = logging.getLogger(__name__)

class BaseGeoJSONConverter(ABC):
    """Base class for GeoJSON converters."""
    
    def __init__(self, settings: dict = None):
        self.settings = settings or {}
    
    @abstractmethod
    def convert(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Convert data to GeoJSON format."""
        pass
    
    def generate_geojson_path(self, region: str, dataset: str, timestamp: str) -> Path:
        """Generate standard output path for GeoJSON files."""
        output_dir = Path(self.settings.get('output_dir', 'output')) / 'geojson' / region / dataset
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir / f"{dataset}_{region}_{timestamp}.geojson"
