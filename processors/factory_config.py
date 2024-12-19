from typing import Dict, Type

from processors.visualization.sst_visualizer import SSTVisualizer
from processors.visualization.currents_visualizer import CurrentsVisualizer
from processors.visualization.chlorophyll_visualizer import ChlorophyllVisualizer
from processors.visualization.waves_visualizer import WavesVisualizer
from processors.visualization.water_movement_visualizer import WaterMovementVisualizer

from processors.geojson.sst_converter import SSTGeoJSONConverter
from processors.geojson.sst_contour_converter import SSTContourConverter
from processors.geojson.currents_converter import CurrentsGeoJSONConverter
from processors.geojson.chlorophyll_converter import ChlorophyllGeoJSONConverter
from processors.geojson.chlorophyll_contour_converter import ChlorophyllContourConverter
from processors.geojson.waves_converter import WavesGeoJSONConverter
from processors.geojson.water_movement_converter import WaterMovementConverter
from processors.geojson.water_movement_contour_converter import WaterMovementContourConverter
from processors.geojson.ocean_features_converter import OceanFeaturesConverter
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
        'converters': {
            'data': WavesGeoJSONConverter
        }
    },
    'currents': {
        'visualizer': CurrentsVisualizer,
        'converters': {
            'data': CurrentsGeoJSONConverter
        }
    }
} 