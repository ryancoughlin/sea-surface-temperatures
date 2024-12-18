from typing import Dict, Type

from processors.visualization.sst_visualizer import SSTVisualizer
from processors.visualization.currents_visualizer import CurrentsVisualizer
from processors.visualization.chlorophyll_visualizer import ChlorophyllVisualizer
from processors.visualization.waves_visualizer import WavesVisualizer
from processors.visualization.ocean_dynamics_visualizer import OceanDynamicsVisualizer

from processors.geojson.sst_converter import SSTGeoJSONConverter
from processors.geojson.sst_contour_converter import SSTContourConverter
from processors.geojson.currents_converter import CurrentsGeoJSONConverter
from processors.geojson.chlorophyll_converter import ChlorophyllGeoJSONConverter
from processors.geojson.chlorophyll_contour_converter import ChlorophyllContourConverter
from processors.geojson.waves_converter import WavesGeoJSONConverter
from processors.geojson.ocean_dynamics_converter import OceanDynamicsGeoJSONConverter
from processors.geojson.ocean_dynamics_contour_converter import OceanDynamicsContourConverter
from processors.geojson.ocean_features_converter import OceanFeaturesConverter

# Map dataset types to their processors
PROCESSOR_MAPPING = {
    'sst': {
        'visualizer': SSTVisualizer,
        'converters': {
            'data': SSTGeoJSONConverter,
            'contours': SSTContourConverter
        }
    },
    'chlorophyll': {
        'visualizer': ChlorophyllVisualizer,
        'converters': {
            'data': ChlorophyllGeoJSONConverter,
            'contours': ChlorophyllContourConverter
        }
    },
    'water_movement': {
        'visualizer': OceanDynamicsVisualizer,
        'converters': {
            'data': OceanDynamicsGeoJSONConverter,
            'contours': OceanDynamicsContourConverter,
            'features': OceanFeaturesConverter
        }
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