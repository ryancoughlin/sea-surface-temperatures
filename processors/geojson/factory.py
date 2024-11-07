from .sst_converter import SSTGeoJSONConverter
from .currents_converter import CurrentsGeoJSONConverter
from .chlorophyll_converter import ChlorophyllGeoJSONConverter
from .contour_converter import ContourConverter
from utils.path_manager import PathManager
from config.settings import SOURCES

class GeoJSONConverterFactory:
    """Factory for creating appropriate GeoJSON converters."""
    
    def __init__(self, path_manager: PathManager):
        self.path_manager = path_manager
    
    def create(self, dataset: str, converter_type: str = 'data'):
        """Create appropriate converter based on dataset type and converter type."""
        try:
            dataset_config = SOURCES[dataset]
            dataset_type = dataset_config['type']
            
            # Special handling for SST contours (only for specific datasets)
            if dataset_type == 'sst':
                if converter_type == 'contour' and dataset in ['LEOACSPOSSTL3SnrtCDaily', 'BLENDEDsstDNDaily']:
                    return ContourConverter(self.path_manager)
                return SSTGeoJSONConverter(self.path_manager)
            
            # Handle other types based on dataset_type
            if dataset_type == 'currents':
                return CurrentsGeoJSONConverter(self.path_manager)
            elif dataset_type == 'chlorophyll':
                return ChlorophyllGeoJSONConverter(self.path_manager)
            else:
                raise ValueError(f"Unknown dataset type: {dataset_type}")
                
        except KeyError:
            raise ValueError(f"Dataset {dataset} not found in SOURCES configuration")
