from typing import Dict, Type
from .base_processor import BaseImageProcessor
from .sst_processor import SSTProcessor
from .currents_processor import CurrentsProcessor
from .chlorophyll import ChlorophyllProcessor
from config.settings import SOURCES
import logging

logger = logging.getLogger(__name__)

class ProcessorFactory:
    """Single factory to create the right processor."""
    
    @classmethod
    def create(cls, dataset: str) -> BaseImageProcessor:
        type = SOURCES[dataset]['type']
        
        processors = {
            'sst': SSTProcessor,
            'currents': CurrentsProcessor,
            'chlorophyll': ChlorophyllProcessor
        }
        
        return processors[type]()