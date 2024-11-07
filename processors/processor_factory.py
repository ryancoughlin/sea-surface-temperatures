from typing import Dict
from .base_processor import BaseImageProcessor
from .sst_processor import SSTProcessor
from .currents_processor import CurrentsProcessor
from .chlorophyll import ChlorophyllProcessor
from utils.path_manager import PathManager
from config.settings import SOURCES

class ProcessorFactory:
    """Factory for creating appropriate data processors based on data type."""
    
    def __init__(self, path_manager: PathManager):
        self.path_manager = path_manager
    
    def create(self, dataset: str) -> BaseImageProcessor:
        """
        Create appropriate processor based on dataset type.
        Both CMEMS and ERDDAP data use the same processors based on type.
        """
        try:
            dataset_config = SOURCES[dataset]
            dataset_type = dataset_config['type']
            
            # Use same processor for each data type regardless of source
            if dataset_type == 'sst':
                return SSTProcessor(self.path_manager)
            elif dataset_type == 'currents':
                return CurrentsProcessor(self.path_manager)
            elif dataset_type == 'chlorophyll':
                return ChlorophyllProcessor(self.path_manager)
            else:
                raise ValueError(f"Unknown dataset type: {dataset_type}")
                
        except KeyError:
            raise ValueError(f"Dataset {dataset} not found in SOURCES configuration")