import logging
import aiohttp
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Union, Any
from config.settings import SOURCES
from config.regions import REGIONS
from utils.path_manager import PathManager
from utils.dates import DateFormatter
import ssl

logger = logging.getLogger(__name__)

class ERDDAPError(Exception):
    """Custom exception for ERDDAP-related errors"""
    def __init__(self, message: str, dataset: str, reason: str, details: dict = None):
        self.dataset = dataset
        self.reason = reason
        self.details = details or {}
        super().__init__(message)

class ERDDAPService:
    """Service for fetching oceanographic data from ERDDAP servers"""
    
    def __init__(self, session: aiohttp.ClientSession, path_manager: PathManager):
        self.session = session
        self.path_manager = path_manager
        self.date_formatter = DateFormatter()
        
        # Simplified configuration
        self.timeout = aiohttp.ClientTimeout(total=120)  # 2 minutes
        self.headers = {
            'Accept': 'application/x-netcdf, */*',
            'Accept-Encoding': 'gzip, deflate',
        }

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
        """Build ERDDAP request URL matching NOAA's format"""
        url = f"{base_url}/{dataset_id}{file_type}?"
        
        var_constraints = []
        for var in variables:
            var_url = var
            
            # Time constraint with NOAA's encoding format
            time_values = constraints.get('time')
            time_start = time_values.get('start')
            time_stop = time_values.get('stop')
            # Note the %5B and %5D for brackets, and keeping () unencoded
            time_constraint = f"%5B({time_start}):1:({time_stop})%5D"
            
            # Lat/lon constraints with same encoding
            lat_values = constraints.get('latitude')
            lon_values = constraints.get('longitude')
            lat_constraint = f"%5B({lat_values.get('start')}):1:({lat_values.get('stop')})%5D"
            lon_constraint = f"%5B({lon_values.get('start')}):1:({lon_values.get('stop')})%5D"
            
            var_url += time_constraint + lat_constraint + lon_constraint
            var_constraints.append(var_url)
            
        url += ','.join(var_constraints)
        return url

    async def save_data(self, date: datetime, dataset: str, region_id: str) -> Path:
        """Fetch and save data with built-in rate limiting"""
        output_path = self.path_manager.get_data_path(date, dataset, region_id)
        
        if output_path.exists():
            logger.info(f"📂 Using cached data")
            logger.info(f"   └── 📄 {output_path.name}")
            return output_path

        logger.info(f"⬇️  Downloading new ERDDAP data")
        logger.info(f"   ├── 📦 {dataset}")
        logger.info(f"   ├── 🌎 {region_id}")
        logger.info(f"   └── 📅 {date.strftime('%Y-%m-%d')}")

        try:
            # Get source configuration using dataset name
            source_config = SOURCES[dataset]
            if not source_config:
                raise ValueError(f"No configuration found for dataset {dataset}")

            # Get standardized query date
            lag_days = source_config.get('lag_days', 1)
            query_date = self.date_formatter.get_query_date(date, lag_days)
            
            region = REGIONS[region_id]
            output_path = self.path_manager.get_data_path(date, dataset, region_id)
            
            # Build constraints with standardized date format
            constraints = {
                'time': {
                    'start': self.date_formatter.format_erddap_date(query_date),
                    'stop': self.date_formatter.format_erddap_date(query_date)
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

            # Build URL first so it's available for error logging
            url = self.build_url(
                base_url=source_config['base_url'],
                dataset_id=source_config['dataset_id'],
                variables=source_config.get('variables', []),
                constraints=constraints,
                file_type='.nc'
            )

            async with self.session.get(
                url, 
                headers=self.headers,
                timeout=self.timeout,
                ssl=False  # Simplified SSL handling
            ) as response:
                if response.ok:
                    data = await response.read()
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_bytes(data)
                    
                    size_mb = len(data)/1024/1024
                    logger.info(f"✅ Download complete")
                    logger.info(f"   ├── 📊 Size: {size_mb:.2f}MB")
                    logger.info(f"   └── 💾 Saved to: {output_path.name}")
                    return output_path
                
                error_text = await response.text()
                raise ERDDAPError(
                    message=f"ERDDAP request failed: Status {response.status}",
                    dataset=dataset,
                    reason=error_text[:500],
                    details={
                        'status': response.status,
                        'url': url,
                        'region': region_id,
                        'date': date.strftime('%Y-%m-%d')
                    }
                )

        except asyncio.TimeoutError as e:
            raise ERDDAPError(
                message=f"Download timed out after {self.timeout.total} seconds",
                dataset=dataset,
                reason="timeout",
                details={'timeout': self.timeout.total, 'url': url}
            ) from e

        except aiohttp.ClientError as e:
            raise ERDDAPError(
                message=str(e),
                dataset=dataset,
                reason="connection_error",
                details={'url': url}
            ) from e
