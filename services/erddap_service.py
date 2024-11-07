import logging
import aiohttp
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Union, Any
from config.settings import SOURCES
from config.regions import REGIONS
from utils.path_manager import PathManager

logger = logging.getLogger(__name__)

class ERDDAPService:
    """Service for fetching oceanographic data from ERDDAP servers"""
    
    def __init__(self, session: aiohttp.ClientSession, path_manager: PathManager):
        # Use aiohttp's built-in connection pooling and timeout handling
        self.session = session
        self.path_manager = path_manager
        self.timeout = aiohttp.ClientTimeout(total=30)  # 30 second timeout

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

    async def save_data(self, date: datetime, dataset: str, region_id: str) -> Path:
        """Fetch and save data with built-in rate limiting"""
        url = None
        try:
            # Get source configuration using dataset name
            source_config = SOURCES[dataset]
            if not source_config:
                raise ValueError(f"No configuration found for dataset {dataset}")

            # Calculate time range using lag_days
            lag_days = source_config.get('lag_days', 1)
            offset_date = date - timedelta(days=lag_days)
            region = REGIONS[region_id]
            
            # Get data path first to ensure date is valid
            output_path = self.path_manager.get_data_path(
                date=date,
                dataset=dataset,
                region=region_id
            )
            
            # Build constraints using ISO format for ERDDAP
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

            # Add altitude if present in source config
            if 'altitude' in source_config:
                constraints['altitude'] = source_config['altitude']

            # Build download URL using existing method
            url = self.build_url(
                base_url=source_config['base_url'],
                dataset_id=source_config['dataset_id'],
                variables=source_config.get('variables', []),
                constraints=constraints,
                file_type='.nc'
            )

            # Use aiohttp's retry and timeout handling
            async with self.session.get(url, timeout=self.timeout) as response:
                response.raise_for_status()
                data = await response.read()
                
                # Save data
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(data)
                
                logger.info(f"Successfully saved {len(data)/1024/1024:.2f}MB to {output_path}")
                return output_path

        except Exception as e:
            if url:  # Only log URL if we got far enough to create it
                logger.error(f"Failed URL: {url}")
            logger.error(f"Failed to fetch data: {str(e)}")
            raise
