import logging
import aiohttp
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Union
from config.settings import SOURCES
logger = logging.getLogger(__name__)

class ERDDAPService:
    """Service for fetching oceanographic data from ERDDAP servers"""
    
    def __init__(self):
        self.clients = {}
        
    def build_constraint(self, 
                        dim_name: str,
                        start: Optional[Union[str, float]] = None,
                        stop: Optional[Union[str, float]] = None,
                        stride: Optional[int] = None) -> str:
        """Build a dimension constraint string"""
        if dim_name == 'time':
            return f"[({start}):({stop})]" if start and stop else "[(last)]"
            
        if all(v is not None for v in [start, stop]):
            # Fix double colon issue - only add stride if it's specified
            stride_str = f":{stride}" if stride is not None else ""
            return f"[{start}{stride_str}:{stop}]"  # Removed extra colon
        
        return "[]"

    def build_url(self, 
                  base_url: str,
                  dataset_id: str,
                  variables: list,
                  constraints: Dict,
                  file_type: str = '.nc') -> str:
        """Build ERDDAP request URL"""
        url = f"{base_url}/{dataset_id}{file_type}?"
        
        var_constraints = []
        for var in variables:
            var_url = var
            
            # Add time constraint first
            time_values = constraints.get('time')
            time_constraint = self.build_constraint(
                dim_name='time',
                start=time_values.get('start'),
                stop=time_values.get('stop')
            )
            var_url += time_constraint
            
            # Add altitude if present (second position)
            if 'altitude' in constraints:
                var_url += constraints['altitude']
            
            # Add lat/lon constraints
            lat_values = constraints.get('latitude')
            lon_values = constraints.get('longitude')
            var_url += f"[({lat_values.get('start')}):1:({lat_values.get('stop')})]"
            var_url += f"[({lon_values.get('start')}):1:({lon_values.get('stop')})]"
            
            var_constraints.append(var_url)
            
        url += ','.join(var_constraints)
        return url

    async def save_data(self,
                       date: datetime,
                       dataset: Dict,
                       region: Dict,
                       output_path: Path) -> Path:
        """Fetch and save data to NetCDF file"""
        try:
            # Get source configuration
            source_config = next(
                (config for _, config in SOURCES.items() 
                 if config.get('dataset_id') == dataset.get('dataset_id')),
                None
            )
            
            if not source_config:
                raise ValueError(f"No configuration found for dataset {dataset.get('dataset_id')}")

            # Calculate time range using lag_days
            today = datetime.utcnow()
            offset_date = today - timedelta(days=source_config.get('lag_days', 1))
            # Build constraints
            constraints = {
                'time': {
                    'start': offset_date.strftime('%Y-%m-%dT00:00:00Z'),
                    'stop': offset_date.strftime('%Y-%m-%dT00:00:00Z')
                },
                'latitude': {
                    'start': region['bounds'][0][1],
                    'stop': region['bounds'][1][1]
                },
                'longitude': {
                    'start': region['bounds'][0][0],
                    'stop': region['bounds'][1][0]
                }
            }

            # Add altitude constraint if present in source config
            if 'altitude' in source_config:
                constraints['altitude'] = source_config['altitude']

            # Build download URL
            url = self.build_url(
                base_url=source_config['base_url'],
                dataset_id=source_config['dataset_id'],
                variables=source_config.get('variables', []),
                constraints=constraints,
                file_type='.nc'
            )

            # Prepare output path
            output_dir = output_path / region['name'].lower().replace(" ", "_") / source_config['name'].lower().replace(" ", "_")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"{source_config['dataset_id']}_{region['name'].lower().replace(' ', '_')}_{date.strftime('%Y%m%d')}.nc"

            # Download file using aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        raise Exception(f"Failed to download data: {response.status}")
                    
                    with open(output_file, 'wb') as f:
                        while True:
                            chunk = await response.content.read(8192)
                            if not chunk:
                                break
                            f.write(chunk)

            logger.info(f"Successfully saved data to {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Error saving ERDDAP data: {e}")
            logger.error(f"Failed URL: {url if 'url' in locals() else 'URL not available'}")
            raise

