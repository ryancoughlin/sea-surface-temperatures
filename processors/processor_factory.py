from typing import Dict, Type
from .base_processor import BaseImageProcessor
from .sst_processor import SSTProcessor
from .currents_processor import CurrentsProcessor
from config.settings import SOURCES
import logging

logger = logging.getLogger(__name__)

class ProcessorFactory:
    """Single factory to create the right processor."""
    
    @classmethod
    def create(cls, dataset: str) -> BaseImageProcessor:
        category = SOURCES[dataset]['category']
        
        processors = {
            'sst': SSTProcessor,
            'currents': CurrentsProcessor
        }
        
        return processors[category]()