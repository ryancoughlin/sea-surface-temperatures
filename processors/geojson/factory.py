from .sst_converter import SSTGeoJSONConverter
from .currents_converter import CurrentsGeoJSONConverter
from .chlorophyll_converter import ChlorophyllGeoJSONConverter
from .contour_converter import ContourConverter
from utils.path_manager import PathManager

class GeoJSONConverterFactory:
    """Factory for creating appropriate GeoJSON converters."""
    
    def __init__(self, path_manager: PathManager):
        self.path_manager = path_manager
    
    def create(self, dataset: str, converter_type: str = 'data'):
        """Create appropriate converter based on dataset type and converter type."""
        # Handle both SST datasets
        if dataset in ['LEOACSPOSSTL3SnrtCDaily', 'BLENDEDsstDNDaily']:
            if converter_type == 'contour':
                return ContourConverter(self.path_manager)
            return SSTGeoJSONConverter(self.path_manager)
        elif dataset == 'BLENDEDNRTcurrentsDaily':
            return CurrentsGeoJSONConverter(self.path_manager)
        elif dataset == 'chlorophyll_oci':
            return ChlorophyllGeoJSONConverter(self.path_manager)
        else:
            raise ValueError(f"No converter available for dataset: {dataset}")
