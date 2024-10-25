import xarray as xr
import numpy as np
import json
import logging
from pathlib import Path
from config import settings
from config.settings import SOURCES

class GeoJSONConverter:
    def convert_to_geojson(self, data, region: str, dataset: str, timestamp: str) -> Path:
        """Convert data to GeoJSON format."""
        try:
            category = SOURCES[dataset]['category']
            
            # Simple category check
            if category == 'currents':
                # Handle vector data (u, v components)
                u = data['u'].values
                v = data['v'].values
                
                # Calculate magnitude for the feature properties
                magnitude = np.sqrt(u**2 + v**2)
                
                # Create features with both magnitude and direction
                features = []
                for i in range(0, u.shape[0], stride):
                    for j in range(0, u.shape[1], stride):
                        lon = float(data['u'].lon[j])
                        lat = float(data['u'].lat[i])
                        
                        feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [lon, lat]
                            },
                            "properties": {
                                "magnitude": float(magnitude[i, j]),
                                "u": float(u[i, j]),
                                "v": float(v[i, j])
                            }
                        }
                        features.append(feature)
                
            else:
                # Handle scalar data (sst, chlorophyll)
                # Existing code for scalar data
                features = []
                for i in range(0, data.shape[0], stride):
                    for j in range(0, data.shape[1], stride):
                        lon = float(data.lon[j])
                        lat = float(data.lat[i])
                        value = float(data[i, j])
                        
                        if not np.isnan(value):
                            feature = {
                                "type": "Feature",
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [lon, lat]
                                },
                                "properties": {
                                    "value": value
                                }
                            }
                            features.append(feature)
            
            # Create the GeoJSON structure
            geojson = {
                "type": "FeatureCollection",
                "features": features
            }
            
            # Save to file
            output_path = self.generate_geojson_path(region, dataset, timestamp)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(geojson, f)
            
            return output_path
            
        except Exception as e:
            logging.error(f"Error converting to GeoJSON: {str(e)}")
            raise

    def generate_geojson_path(self, region: str, dataset: str, timestamp: str) -> Path:
        """Generate the file path for the GeoJSON file."""
        return settings.REGIONS_DIR / region / "datasets" / dataset / "dates" / timestamp / "geojson.geojson"
