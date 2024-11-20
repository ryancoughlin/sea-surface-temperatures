from typing import Dict, Type
from .base_converter import BaseGeoJSONConverter
from .sst_converter import SSTGeoJSONConverter
from .currents_converter import CurrentsGeoJSONConverter
from .chlorophyll_converter import ChlorophyllGeoJSONConverter
from .waves_converter import WavesGeoJSONConverter
from .contour_converter import ContourConverter
from config.settings import SOURCES

class GeoJSONConverterFactory:
    def __init__(self, path_manager):
        self.path_manager = path_manager
        self._converters = {
            'data': {
                'sst': SSTGeoJSONConverter,
                'currents': CurrentsGeoJSONConverter,
                'waves': WavesGeoJSONConverter,
                'chlorophyll': ChlorophyllGeoJSONConverter
            },
            'contour': {
                'sst': ContourConverter
            }
        }
    
    def create(self, dataset: str, converter_type: str = 'data') -> BaseGeoJSONConverter:
        dataset_type = SOURCES[dataset]['type']
        converter_class = self._converters.get(converter_type, {}).get(dataset_type)
        if not converter_class:
            raise ValueError(f"No converter found for type: {dataset_type}")
        return converter_class(self.path_manager)
