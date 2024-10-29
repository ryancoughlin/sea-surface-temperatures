from .sst_converter import SSTGeoJSONConverter
from .currents_converter import CurrentsGeoJSONConverter
from .chlorophyll_converter import ChlorophyllGeoJSONConverter

class GeoJSONConverterFactory:
    """Factory for creating appropriate GeoJSON converters."""
    
    def create(self, dataset: str, converter_type: str = 'data'):
        """Create appropriate converter based on dataset type."""
        if dataset == 'LEOACSPOSSTL3SnrtCDaily':
            return SSTGeoJSONConverter()
        elif dataset == 'BLENDEDNRTcurrentsDaily':
            return CurrentsGeoJSONConverter()
        elif dataset == 'chlorophyll_oci':
            return ChlorophyllGeoJSONConverter()
        else:
            raise ValueError(f"No converter available for dataset: {dataset}")
