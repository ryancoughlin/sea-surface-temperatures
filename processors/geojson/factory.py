from typing import Dict
from .base_converter import BaseGeoJSONConverter
from .sst_converter import SSTGeoJSONConverter
from .currents_converter import CurrentsGeoJSONConverter
from .chlorophyll_converter import ChlorophyllGeoJSONConverter
from config.settings import SOURCES

class GeoJSONConverterFactory:
    """Factory for creating appropriate GeoJSON converters."""
    
    @classmethod
    def create(cls, dataset: str) -> BaseGeoJSONConverter:
        """Create appropriate GeoJSON converter based on dataset type."""
        dataset_config = SOURCES[dataset]
        category = dataset_config.get('category')
        
        converters = {
            'sst': SSTGeoJSONConverter,
            'currents': CurrentsGeoJSONConverter,
            'chlorophyll': ChlorophyllGeoJSONConverter
        }
        
        converter_class = converters.get(category)
        if not converter_class:
            raise ValueError(f"No GeoJSON converter available for category: {category}")
            
        return converter_class()
