from typing import Dict, Type, Optional
from .base_converter import BaseGeoJSONConverter
from .sst_converter import SSTGeoJSONConverter
from .currents_converter import CurrentsGeoJSONConverter
from .chlorophyll_converter import ChlorophyllGeoJSONConverter
from .chlorophyll_contour_converter import ChlorophyllContourConverter
from .waves_converter import WavesGeoJSONConverter
from .sst_contour_converter import SSTContourConverter
from .ocean_dynamics_converter import OceanDynamicsGeoJSONConverter, OceanDynamicsContourConverter
from .ocean_features_converter import OceanFeaturesConverter
from config.settings import SOURCES
from processors.data.data_assembler import DataAssembler
from utils.path_manager import PathManager


class ConverterType:
    DATA = 'data'
    CONTOUR = 'contour'
    FEATURES = 'features'

class GeoJSONConverterFactory:
    def __init__(self, path_manager: PathManager, data_assembler: Optional[DataAssembler] = None):
        self.path_manager = path_manager
        self.data_assembler = data_assembler
        self._converters: Dict[str, Dict[str, Type[BaseGeoJSONConverter]]] = {
            ConverterType.DATA: {
                'sst': SSTGeoJSONConverter,
                'currents': CurrentsGeoJSONConverter,
                'waves': WavesGeoJSONConverter,
                'chlorophyll': ChlorophyllGeoJSONConverter,
                'water_movement': OceanDynamicsGeoJSONConverter
            },
            ConverterType.CONTOUR: {
                'sst': SSTContourConverter,
                'chlorophyll': ChlorophyllContourConverter,
                'water_movement': OceanDynamicsContourConverter
            },
            ConverterType.FEATURES: {
                'water_movement': OceanFeaturesConverter
            }
        }
    
    def create(self, dataset: str, converter_type: str = ConverterType.DATA) -> BaseGeoJSONConverter:
        """
        Create a GeoJSON converter for the specified dataset and type.
        
        Args:
            dataset: The dataset identifier
            converter_type: Type of converter (data, contour, or features)
            
        Returns:
            BaseGeoJSONConverter: The appropriate converter instance
            
        Raises:
            ValueError: If no converter is found for the dataset type
        """
        dataset_type = SOURCES[dataset]['type']
        converter_class = self._converters.get(converter_type, {}).get(dataset_type)
        
        if not converter_class:
            raise ValueError(
                f"No converter found for dataset_type: {dataset_type}, "
                f"converter_type: {converter_type}"
            )
            
        return converter_class(self.path_manager, self.data_assembler)
