from typing import Dict, Any
from pathlib import Path
from datetime import datetime

class ProcessingConfig:
    """Configuration settings for data processing"""
    
    def __init__(self, 
                 date: datetime,
                 region_id: str,
                 dataset: str,
                 skip_geojson: bool = False):
        self.date = date
        self.region_id = region_id
        self.dataset = dataset
        self.skip_geojson = skip_geojson
        
    @property
    def is_large_region(self) -> bool:
        """Check if this is a large region that needs special handling"""
        return self.region_id == "united_states"
    
    def should_skip_geojson(self) -> bool:
        """Determine if GeoJSON generation should be skipped"""
        return self.skip_geojson or self.is_large_region
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary for logging"""
        return {
            "date": self.date.isoformat(),
            "region_id": self.region_id,
            "dataset": self.dataset,
            "skip_geojson": self.should_skip_geojson()
        } 