from typing import Dict, Any, Optional, List
import numpy as np
import xarray as xr
from pathlib import Path
from datetime import datetime
import logging

from .base_converter import BaseGeoJSONConverter
from config.settings import SOURCES

logger = logging.getLogger(__name__)

class SSTGeoJSONConverter(BaseGeoJSONConverter):
    def _process_temperature_breaks(self, data: xr.DataArray, 
                                  gradient: Optional[xr.DataArray] = None,
                                  front_position: Optional[xr.DataArray] = None,
                                  wind_speed: Optional[xr.DataArray] = None) -> List[Dict]:
        """Process temperature breaks with available data."""
        features = []
        
        if gradient is None:
            return features
            
        # Calculate break thresholds from gradient data
        strong_threshold = np.nanpercentile(gradient, 95)
        moderate_threshold = np.nanpercentile(gradient, 85)
        
        # Find contours of gradient magnitude
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots()
        contours = ax.contour(gradient)
        plt.close(fig)
        
        for level_idx, level_value in enumerate(contours.levels):
            for segment in contours.collections[level_idx].get_paths():
                coords = segment.vertices
                
                # Skip if too short
                if len(coords) < 5:
                    continue
                
                # Determine break strength
                avg_gradient = float(np.mean(level_value))
                if avg_gradient >= strong_threshold:
                    strength = "strong"
                elif avg_gradient >= moderate_threshold:
                    strength = "moderate"
                else:
                    strength = "weak"
                
                # Get front confidence if available
                front_confidence = None
                if front_position is not None:
                    # Sample front_position along the contour
                    front_values = front_position.interp(
                        longitude=xr.DataArray(coords[:, 0]),
                        latitude=xr.DataArray(coords[:, 1])
                    )
                    front_confidence = float(np.mean(front_values))
                
                # Add break stability if wind data available
                stability = None
                if wind_speed is not None:
                    avg_wind = float(np.mean(wind_speed.interp(
                        longitude=xr.DataArray(coords[:, 0]),
                        latitude=xr.DataArray(coords[:, 1])
                    )))
                    
                    if avg_wind > 15:
                        stability = "established"
                    elif avg_wind > 8:
                        stability = "probable"
                    else:
                        stability = "temporary"
                
                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": coords.tolist()
                    },
                    "properties": {
                        "type": "temperature_break",
                        "strength": strength,
                        "gradient": float(avg_gradient),
                        "front_confidence": front_confidence,
                        "stability": stability
                    }
                })
        
        return features

    def _process_species_zones(self, data: xr.DataArray, 
                             fishing_zones: Dict) -> List[Dict]:
        """Process temperature-based species zones."""
        features = []
        
        if not fishing_zones:
            return features
            
        for species, info in fishing_zones.get("species_temps", {}).items():
            temp_range = info["range"]
            
            # Create mask for species temperature range
            species_mask = (data >= temp_range[0]) & (data <= temp_range[1])
            
            # Convert masked areas to polygons
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots()
            contours = ax.contour(species_mask)
            plt.close(fig)
            
            for collection in contours.collections:
                for path in collection.get_paths():
                    coords = path.vertices
                    if len(coords) < 4:
                        continue
                        
                    features.append({
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [coords.tolist()]
                        },
                        "properties": {
                            "type": "species_zone",
                            "species": species,
                            "temp_range": temp_range,
                            "break_preference": info["break_preference"]
                        }
                    })
        
        return features

    def convert(self, data_path: Path, region: str, dataset: str, date: datetime) -> Path:
        """Convert SST data to enhanced fishing-oriented GeoJSON format."""
        try:
            ds = self.load_dataset(data_path)
            source_config = SOURCES[dataset]
            
            # Get base SST data
            sst_data = self.normalize_dataset(ds, source_config['variables'][0])
            
            # Optional additional variables
            gradient = None
            front_position = None
            wind_speed = None
            
            if 'sst_gradient_magnitude' in source_config['variables']:
                gradient = self.normalize_dataset(ds, 'sst_gradient_magnitude')
            
            if 'sst_front_position' in source_config['variables']:
                front_position = self.normalize_dataset(ds, 'sst_front_position')
                
            if 'wind_speed' in source_config['variables']:
                wind_speed = self.normalize_dataset(ds, 'wind_speed')
            
            # Process different layer types
            features = []
            
            # Add temperature breaks if gradient data available
            if gradient is not None:
                features.extend(self._process_temperature_breaks(
                    sst_data, gradient, front_position, wind_speed
                ))
            
            # Add species zones if configured
            if "fishing_zones" in source_config:
                features.extend(self._process_species_zones(
                    sst_data, source_config["fishing_zones"]
                ))
            
            # Create GeoJSON structure
            geojson = {
                "type": "FeatureCollection",
                "features": features,
                "properties": {
                    "dataset": dataset,
                    "date": date.isoformat(),
                    "region": region,
                    "available_layers": [
                        layer for layer in ["temperature_breaks", "species_zones"]
                        if any(f["properties"]["type"] == layer.rstrip("s") for f in features)
                    ]
                }
            }
            
            # Save and return
            output_path = self.path_manager.get_asset_paths(date, dataset, region).data
            self.save_geojson(geojson, output_path)
            return output_path
            
        except Exception as e:
            logger.error(f"Error converting SST data: {str(e)}")
            logger.exception(e)
            raise
