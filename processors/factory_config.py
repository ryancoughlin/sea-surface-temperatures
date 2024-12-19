from typing import Dict, Type

# SST
from processors.visualization.sst_visualizer import SSTVisualizer
from processors.geojson.sst_converter import SSTGeoJSONConverter
from processors.geojson.sst_contour_converter import SSTContourConverter

# Currents
from processors.visualization.currents_visualizer import CurrentsVisualizer
from processors.geojson.currents_converter import CurrentsGeoJSONConverter

# Chlorophyll
from processors.visualization.chlorophyll_visualizer import ChlorophyllVisualizer
from processors.geojson.chlorophyll_converter import ChlorophyllGeoJSONConverter
from processors.geojson.chlorophyll_contour_converter import ChlorophyllContourConverter

# Waves
from processors.visualization.waves_visualizer import WavesVisualizer
from processors.geojson.waves_converter import WavesGeoJSONConverter

# Water Movement
from processors.visualization.water_movement_visualizer import WaterMovementVisualizer
from processors.geojson.water_movement_converter import WaterMovementConverter
from processors.geojson.water_movement_contour_converter import WaterMovementContourConverter
from processors.geojson.fishing_spots_converter import FishingSpotConverter
# Map dataset types to their processors
PROCESSOR_MAPPING = {
    'sst': {
        'visualizer': SSTVisualizer,
        'geojson': SSTGeoJSONConverter,
        'contours': SSTContourConverter
    },
    'water_movement': {
        'visualizer': WaterMovementVisualizer,
        'geojson': WaterMovementConverter,
        'contours': WaterMovementContourConverter,
        'features': FishingSpotConverter
    },
    'waves': {
        'visualizer': WavesVisualizer,
        'geojson': WavesGeoJSONConverter
    },
    'currents': {
        'visualizer': CurrentsVisualizer,
        'geojson': CurrentsGeoJSONConverter
    },
    'chlorophyll': {
        'visualizer': ChlorophyllVisualizer,
        'geojson': ChlorophyllGeoJSONConverter,
        'contours': ChlorophyllContourConverter
    }
} 