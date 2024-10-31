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
        # Create connector with rate limiting
        connector = aiohttp.TCPConnector(
            limit=3,  # Max 3 concurrent connections
            limit_per_host=2,  # Max 2 concurrent connections per host
            force_close=True  # Close connection after each request
        )
        
        self.session = session
        self.path_manager = path_manager
        self.max_retries = 3
        self.retry_delay = 5  # Increased to 5 seconds between retries
        self.timeout = aiohttp.ClientTimeout(
            total=300,  # 5 min total timeout
            connect=60,  # 60 seconds to establish connection
            sock_read=60  # 60 seconds to read response chunk
        )
        
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
                       region_id: str) -> Path:
        """Fetch and save data with built-in rate limiting"""
        url = None
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
            lag_days = source_config.get('lag_days', 1)
            offset_date = date - timedelta(days=lag_days)
            region = REGIONS[region_id]
            
            # Get data path first to ensure date is valid
            output_path = self.path_manager.get_data_path(
                date=date,
                dataset=dataset['dataset_id'],
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

            # Write directly to output_path instead of constructing new path
            for attempt in range(self.max_retries):
                try:
                    async with self.session.get(url, timeout=self.timeout) as response:
                        if response.status == 429:  # Rate limit hit
                            retry_after = int(response.headers.get('Retry-After', self.retry_delay))
                            logger.warning(f"Rate limit exceeded, waiting {retry_after}s")
                            await asyncio.sleep(retry_after)
                            break  # Exit the with block
                        
                        if response.status == 200:
                            content_length = response.headers.get('Content-Length')
                            if content_length:
                                expected_size = int(content_length)
                                logger.info(f"Downloading {expected_size/1024/1024:.2f}MB from ERDDAP")
                            
                            # Ensure directory exists
                            output_path.parent.mkdir(parents=True, exist_ok=True)
                            bytes_downloaded = 0
                            
                            with open(output_path, 'wb') as f:
                                while True:
                                    try:
                                        chunk = await response.content.read(8192)
                                        if not chunk:
                                            break
                                        bytes_downloaded += len(chunk)
                                        f.write(chunk)
                                    except asyncio.TimeoutError:
                                        logger.error(f"Timeout while downloading chunk after {bytes_downloaded/1024/1024:.2f}MB")
                                        raise
                            
                            logger.info(f"Successfully saved {bytes_downloaded/1024/1024:.2f}MB to {output_path}")
                            return output_path
                        
                        elif response.status == 404:
                            logger.error(f"404 – Not found on ERDDAP server: {url}")
                            raise aiohttp.ClientError("Dataset not found")
                        elif response.status >= 500:
                            logger.error(f"ERDDAP server error {response.status}: {await response.text()}")
                            raise aiohttp.ClientError(f"Server error: {response.status}")
                        else:
                            logger.error(f"Failed to download data: HTTP {response.status}")
                            logger.error(f"Response: {await response.text()}")
                            raise aiohttp.ClientError(f"HTTP {response.status}")

                except Exception as e:
                    logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.retry_delay)
                        continue
                    raise

        except Exception as e:
            if url:  # Only log URL if we got far enough to create it
                logger.error(f"Failed URL: {url}")
            logger.error(f"Error saving ERDDAP data: {str(e)}")
            raise
