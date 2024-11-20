from typing import Dict, Type
from .base_processor import BaseImageProcessor
from .sst_processor import SSTProcessor
from .currents_processor import CurrentsProcessor
from .chlorophyll import ChlorophyllProcessor
from .waves_processor import WavesProcessor
from utils.path_manager import PathManager
from config.settings import SOURCES

class ProcessorFactory:
    """Factory for creating appropriate data processors based on data type."""
    
    def __init__(self, path_manager: PathManager):
        self.path_manager = path_manager
        self.processors = {
            'sst': SSTProcessor(path_manager),
            'currents': CurrentsProcessor(path_manager),
            'waves': WavesProcessor(path_manager),
            'chlorophyll': ChlorophyllProcessor(path_manager)
        }
    
    def create(self, dataset_type: str) -> BaseImageProcessor:
        """Create a processor instance for the given dataset type."""
        if dataset_type not in self.processors:
            raise ValueError(f"Processor type {dataset_type} not supported")
        return self.processors[dataset_type]