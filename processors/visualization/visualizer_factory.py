from typing import Dict, Type
from .base_visualizer import BaseVisualizer
from utils.path_manager import PathManager
from ..factory_config import PROCESSOR_MAPPING
import logging

logger = logging.getLogger(__name__)

class VisualizerFactory:
    """Factory for creating appropriate visualizers based on data type."""
    
    def __init__(self, path_manager: PathManager):
        self.path_manager = path_manager

    def create(self, dataset_type: str) -> BaseVisualizer:
        """Create a visualizer instance for the given dataset type."""
        if dataset_type not in PROCESSOR_MAPPING:
            raise ValueError(f"Visualizer type {dataset_type} not supported")
            
        logger.info(f"Creating visualizer for {dataset_type}")
        visualizer_class = PROCESSOR_MAPPING[dataset_type]['visualizer']
        return visualizer_class(self.path_manager)