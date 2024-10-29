import xarray as xr
import numpy as np
from pathlib import Path
import json
import logging
from typing import List, Dict, Any, Tuple
from config.settings import SOURCES
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

def open_dataset(data_path: Path, dataset: str) -> Tuple[xr.Dataset, List[str]]:
    """Common dataset opening with proper variable extraction"""
    try:
        ds = xr.open_dataset(data_path)
        variables = SOURCES[dataset]['variables']
        return ds, variables
    except Exception as e:
        logger.error(f"Error opening dataset {data_path}: {e}")
        raise

def get_coordinates(ds: xr.Dataset) -> Tuple[str, str]:
    """Get correct coordinate names regardless of lon/lat naming"""
    lon_name = 'longitude' if 'longitude' in ds.coords else 'lon'
    lat_name = 'latitude' if 'latitude' in ds.coords else 'lat'
    return lon_name, lat_name

def create_feature(lon: float, lat: float, properties: Dict[str, Any]) -> Dict:
    """Create a GeoJSON feature with given properties"""
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [lon, lat]
        },
        "properties": properties
    }

def save_geojson(features: List[Dict], output_path: Path, metadata: Dict = None) -> None:
    """Save features to GeoJSON with optional metadata"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    if metadata:
        geojson["metadata"] = metadata
        
    with open(output_path, 'w') as f:
        json.dump(geojson, f)

def convert_sst(data_path: Path, config: Dict) -> Path:
    """Convert SST data to GeoJSON"""
    ds, variables = open_dataset(data_path, config['dataset'])
    data = ds[variables[0]]  # SST only has one variable
    
    if 'time' in data.dims:
        data = data.isel(time=0)
    
    lon_name, lat_name = get_coordinates(ds)
    features = []
    
    for i in range(0, len(data[lat_name]), config['decimation']):
        for j in range(0, len(data[lon_name]), config['decimation']):
            try:
                value = float(data.values[i, j])
                if not np.isnan(value):
                    lon = float(data[lon_name][j].values)
                    lat = float(data[lat_name][i].values)
                    features.append(create_feature(lon, lat, {
                        "value": value,
                        "unit": "°F"
                    }))
            except Exception as e:
                logger.warning(f"Error processing SST point ({i},{j}): {e}")
                continue
    
    output_path = Path(f"output/regions/{config['region']}/datasets/{config['dataset']}/{config['timestamp']}/data.geojson")
    save_geojson(features, output_path, {"timestamp": config['timestamp']})
    return output_path

def convert_currents(data_path: Path, config: Dict) -> Path:
    """Convert currents data to GeoJSON"""
    ds, variables = open_dataset(data_path, config['dataset'])
    u = ds[variables[0]]  # u_current
    v = ds[variables[1]]  # v_current
    
    if 'time' in u.dims:
        u = u.isel(time=0)
        v = v.isel(time=0)
    
    lon_name, lat_name = get_coordinates(ds)
    features = []
    
    for i in range(0, len(u[lat_name]), config['decimation']):
        for j in range(0, len(u[lon_name]), config['decimation']):
            try:
                u_val = float(u.values[i, j])
                v_val = float(v.values[i, j])
                if not (np.isnan(u_val) or np.isnan(v_val)):
                    lon = float(u[lon_name][j].values)
                    lat = float(u[lat_name][i].values)
                    speed = np.sqrt(u_val**2 + v_val**2)
                    direction = np.degrees(np.arctan2(v_val, u_val)) % 360
                    
                    if speed > config.get('min_magnitude', 0.05):
                        features.append(create_feature(lon, lat, {
                            "u": u_val,
                            "v": v_val,
                            "speed": float(speed),
                            "direction": float(direction),
                            "unit": "m/s"
                        }))
            except Exception as e:
                logger.warning(f"Error processing current point ({i},{j}): {e}")
                continue
    
    output_path = Path(f"output/regions/{config['region']}/datasets/{config['dataset']}/{config['timestamp']}/data.geojson")
    save_geojson(features, output_path, {"timestamp": config['timestamp']})
    return output_path

def convert_chlorophyll(data_path: Path, config: Dict) -> Path:
    """Convert chlorophyll data to GeoJSON"""
    ds, variables = open_dataset(data_path, config['dataset'])
    data = ds[variables[0]]  # chlor_a
    
    if 'time' in data.dims:
        data = data.isel(time=0)
    if 'altitude' in data.dims:
        data = data.squeeze('altitude')
    
    lon_name, lat_name = get_coordinates(ds)
    features = []
    
    for i in range(0, len(data[lat_name]), config['decimation']):
        for j in range(0, len(data[lon_name]), config['decimation']):
            try:
                value = float(data.values[i, j])
                if not np.isnan(value) and value > 0:
                    lon = float(data[lon_name][j].values)
                    lat = float(data[lat_name][i].values)
                    features.append(create_feature(lon, lat, {
                        "value": value,
                        "unit": "mg/m³"
                    }))
            except Exception as e:
                logger.warning(f"Error processing chlorophyll point ({i},{j}): {e}")
                continue
    
    output_path = Path(f"output/regions/{config['region']}/datasets/{config['dataset']}/{config['timestamp']}/data.geojson")
    save_geojson(features, output_path, {"timestamp": config['timestamp']})
    return output_path

def convert_sst_contours(data_path: Path, config: Dict) -> Path:
    """Convert SST data to contour GeoJSON format"""
    try:
        ds, variables = open_dataset(data_path, config['dataset'])
        data = ds[variables[0]]  # SST variable
        
        if 'time' in data.dims:
            data = data.isel(time=0)
        
        lon_name, lat_name = get_coordinates(ds)
        lon = data[lon_name].values
        lat = data[lat_name].values
        
        # Generate contour levels (every 2°F)
        levels = np.arange(np.floor(data.min().values), 
                          np.ceil(data.max().values), 2)
        
        # Create contours using matplotlib
        contours = plt.contour(lon, lat, data.values, levels=levels)
        
        features = []
        # Convert contour paths to GeoJSON LineString features
        for i, collection in enumerate(contours.collections):
            for path in collection.get_paths():
                vertices = path.vertices
                if len(vertices) > 1:  # Only add if we have a valid line
                    features.append({
                        "type": "Feature",
                        "geometry": {
                            "type": "LineString",
                            "coordinates": vertices.tolist()
                        },
                        "properties": {
                            "value": float(levels[i]),
                            "unit": "°F",
                            "isContour": True
                        }
                    })
        
        plt.close()  # Clean up matplotlib figure
        
        output_path = Path(f"output/regions/{config['region']}/datasets/sst_contours/{config['timestamp']}/data.geojson")
        save_geojson(features, output_path, {"timestamp": config['timestamp']})
        return output_path
            
    except Exception as e:
        logger.error(f"Error converting SST contours: {str(e)}")
        raise

# Mapping of dataset IDs to converter functions
CONVERTERS = {
    'LEOACSPOSSTL3SnrtCDaily': convert_sst,
    'sst_contours': convert_sst_contours,
    'BLENDEDNRTcurrentsDaily': convert_currents,
    'chlorophyll_oci': convert_chlorophyll
}