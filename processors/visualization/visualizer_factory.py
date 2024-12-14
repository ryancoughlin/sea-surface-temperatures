from typing import Dict, Type
from .base_visualizer import BaseVisualizer
from .sst_visualizer import SSTVisualizer
from .currents_visualizer import CurrentsVisualizer
from .chlorophyll_visualizer import ChlorophyllVisualizer
from .waves_visualizer import WavesVisualizer
from utils.path_manager import PathManager
from config.settings import SOURCES

class VisualizerFactory:
    """Factory for creating appropriate visualizers based on data type."""
    
    def __init__(self, path_manager: PathManager):
        self.path_manager = path_manager
        self.visualizers = {
            'sst': SSTVisualizer(path_manager),
            'currents': CurrentsVisualizer(path_manager),
            'waves': WavesVisualizer(path_manager),
            'chlorophyll': ChlorophyllVisualizer(path_manager)
        }
    
    def create(self, dataset_type: str) -> BaseVisualizer:
        """Create a visualizer instance for the given dataset type."""
        if dataset_type not in self.visualizers:
            raise ValueError(f"Visualizer type {dataset_type} not supported")
        return self.visualizers[dataset_type]