from typing import Dict, Type, Optional
from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES
from processors.data.data_assembler import DataAssembler
from utils.path_manager import PathManager
from ..factory_config import PROCESSOR_MAPPING
import logging

logger = logging.getLogger(__name__)

class GeoJSONConverterFactory:
    """Factory for creating GeoJSON converters."""
    
    def __init__(self, path_manager: PathManager, data_assembler: Optional[DataAssembler] = None):
        self.path_manager = path_manager
        self.data_assembler = data_assembler

    def create(self, dataset: str, layer_type: str = 'data') -> BaseGeoJSONConverter:
        """
        Create a GeoJSON converter for the specified dataset and layer type.
        
        Args:
            dataset: The dataset identifier
            layer_type: Type of layer to generate (geojson, contours, features)
            
        Returns:
            BaseGeoJSONConverter: The appropriate converter instance
            
        Raises:
            ValueError: If no converter is found for the dataset type and layer
        """
        dataset_type = SOURCES[dataset]['type']
        
        if dataset_type not in PROCESSOR_MAPPING:
            raise ValueError(f"Unsupported dataset type: {dataset_type}")
            
        # Map 'data' to 'geojson' for backward compatibility
        if layer_type == 'data':
            layer_type = 'geojson'
            
        if layer_type not in PROCESSOR_MAPPING[dataset_type]:
            raise ValueError(f"Unsupported layer type: {layer_type} for dataset: {dataset_type}")

        converter_class = PROCESSOR_MAPPING[dataset_type][layer_type]
        return converter_class(self.path_manager, self.data_assembler)
