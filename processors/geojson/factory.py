from typing import Dict
from .base_converter import BaseGeoJSONConverter
from .sst_converter import SSTGeoJSONConverter
from .currents_converter import CurrentsGeoJSONConverter
from config.settings import SOURCES

class GeoJSONConverterFactory:
    """Factory for creating appropriate GeoJSON converters."""
    
    @staticmethod
    def create(dataset: str, settings: Dict = None) -> BaseGeoJSONConverter:
        """Create appropriate GeoJSON converter based on dataset type."""
        dataset_config = SOURCES[dataset]
        category = dataset_config.get('category')
        
        if category == 'sst':
            return SSTGeoJSONConverter(settings)
        elif category == 'currents':
            return CurrentsGeoJSONConverter(settings)
        else:
            raise ValueError(f"No GeoJSON converter available for category: {category}")
