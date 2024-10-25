import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
import aiohttp
from urllib.parse import quote

from .base_service import BaseService
from config.settings import SOURCES

logger = logging.getLogger(__name__)

class ERDDAPService(BaseService):
    async def download(self, date: datetime, dataset: Dict, region: Dict, output_path: Path) -> Path:
        """Download data from ERDDAP.
        
        Args:
            date: Target date for data download
            dataset: Dataset configuration dictionary
            region: Region configuration dictionary 
            output_path: Base path for saving downloaded files

        Returns:
            Path to downloaded file

        Raises:
            ValueError: If dataset configuration is invalid
            RuntimeError: If download fails
        """
        try:
            # Find source config
            source_config = None
            for key, config in SOURCES.items():
                if config.get('dataset_id') == dataset.get('dataset_id'):
                    source_config = config
                    break
            
            if not source_config:
                raise ValueError(
                    f"No source found for dataset_id '{dataset.get('dataset_id')}' in SOURCES configuration. "
                    f"Available dataset_ids: {[s['dataset_id'] for s in SOURCES.values() if 'dataset_id' in s]}"
                )

            # Setup download parameters
            adjusted_date = date - timedelta(days=source_config.get('lag_days', 0))
            time_str = adjusted_date.strftime('%Y-%m-%dT00:00:00Z')
            
            # Extract region bounds
            if not region.get('bounds'):
                raise ValueError(f"Missing bounds for region: {region.get('name')}")
            
            min_lon, min_lat = region['bounds'][0]
            max_lon, max_lat = region['bounds'][1]
            
            # Create output directory
            output_dir = output_path / region['name'].lower().replace(" ", "_") / source_config['name'].lower().replace(" ", "_")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Construct ERDDAP URL
            base_url = source_config.get('base_url', 'https://coastwatch.pfeg.noaa.gov/erddap/griddap')
            dataset_id = source_config['dataset_id']
            
            # Ensure variables is always a list
            variables = source_config.get('variables', [])
            if not variables and source_config.get('variable'):
                variables = source_config['variable'] if isinstance(source_config['variable'], list) else [source_config['variable']]
            
            # Add altitude constraint if present in source config
            altitude_constraint = source_config.get('altitude')
            
            # Build dimension constraints
            time_constraint = f"({quote(time_str)}):1:({quote(time_str)})"
            lat_constraint = f"({min_lat}):1:({max_lat})"
            lon_constraint = f"({min_lon}):1:({max_lon})"
            
            # Add altitude constraint if present in source config
            altitude_constraint = source_config.get('altitude')
            
            # Build complete query for each variable with all dimensions
            var_queries = []
            for var in variables:
                # Start with base dimensions that all datasets have
                dimensions = [
                    f"%5B{time_constraint}%5D",
                ]
                
                # Add altitude dimension if present (with correct formatting)
                if altitude_constraint:
                    # Parse the altitude value from the config string [0:1:0]
                    alt_value = altitude_constraint.strip('[]').split(':')[0]
                    alt_constraint = f"({alt_value}):1:({alt_value})"
                    dimensions.append(f"%5B{alt_constraint}%5D")
                
                # Add lat/lon constraints
                dimensions.extend([
                    f"%5B{lat_constraint}%5D",
                    f"%5B{lon_constraint}%5D"
                ])
                
                # Combine variable name with its dimensions
                var_query = f"{var}{''.join(dimensions)}"
                var_queries.append(var_query)
            
            # Join with comma for multiple variables
            url = f"{base_url}/{dataset_id}.nc?{'%2C'.join(var_queries)}"
            logger.debug(f"ERDDAP request URL: {url}")
            
            # Generate output filename
            output_file = output_dir / f"{dataset_id}_{region['name'].lower().replace(' ', '_')}_{adjusted_date.strftime('%Y%m%d')}.nc"
            
            # Download file
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise RuntimeError(
                            f"ERDDAP download failed with status {response.status}: {error_text}"
                        )
                    
                    with open(output_file, 'wb') as f:
                        while True:
                            chunk = await response.content.read(8192)
                            if not chunk:
                                break
                            f.write(chunk)
            
            logger.info(f"Successfully downloaded {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Error downloading from ERDDAP: {str(e)}")
            raise

    async def download_all_regions(self, date: datetime, dataset: Dict, regions: List[Dict], 
                                 output_path: Path) -> List[Path]:
        """Download data for all regions concurrently with proper error handling."""
        async def download_with_retries(region):
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    return await self.download(date, dataset, region, output_path)
                except Exception as e:
                    if attempt == max_retries - 1:  # Last attempt
                        logger.error(f"Failed all retries for region {region.get('name')}: {str(e)}")
                        raise
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Attempt {attempt + 1} failed for region {region.get('name')}, retrying in {wait_time}s")
                    await asyncio.sleep(wait_time)
        
        # Create tasks for all regions
        tasks = [download_with_retries(region) for region in regions]
        
        # Gather results, allowing individual failures
        results = []
        errors = []
        
        # Wait for all tasks to complete
        done, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        
        for task in done:
            try:
                if not task.exception():
                    results.append(task.result())
            except Exception as e:
                errors.append(e)
                continue
        
        if errors:
            logger.error(f"Completed with {len(errors)} errors out of {len(tasks)} tasks")
        
        return results
