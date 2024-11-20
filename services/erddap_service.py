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
import time

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
        
        # Match curl's working configuration
        self.timeout = aiohttp.ClientTimeout(
            total=300,      # 5 minutes total
            connect=60,     # 60s connect timeout
            sock_read=60    # 60s read timeout
        )
        
        # Match curl's minimal headers
        self.headers = {
            'User-Agent': 'curl/8.7.1',
            'Accept': '*/*'
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

    def build_url(self, date: datetime, dataset: str, region_id: str) -> str:
        """Build ERDDAP request URL with proper encoding"""
        source_config = SOURCES[dataset]
        region = REGIONS[region_id]
        
        # Get query date with lag
        lag_days = source_config.get('lag_days', 1)
        query_date = self.date_formatter.get_query_date(date, lag_days)
        formatted_date = self.date_formatter.format_erddap_date(query_date)
        
        # Build base URL
        base = f"{source_config['base_url']}/{source_config['dataset_id']}.nc?"
        
        # Build variable constraints
        var_parts = []
        for var in source_config.get('variables', []):
            constraints = [
                f"%5B({formatted_date}):1:({formatted_date})%5D",
                f"%5B({region['bounds'][0][1]}):1:({region['bounds'][1][1]})%5D",
                f"%5B({region['bounds'][0][0]}):1:({region['bounds'][1][0]})%5D"
            ]
            var_parts.append(f"{var}{''.join(constraints)}")
            
        return base + ','.join(var_parts)

    async def save_data(self, date: datetime, dataset: str, region_id: str) -> Path:
        """Fetch and save data with built-in rate limiting"""
        output_path = self.path_manager.get_data_path(date, dataset, region_id)
        
        if output_path.exists():
            logger.info(f"📂 Using cached data: {output_path.name}")
            return output_path

        url = self.build_url(date, dataset, region_id)
        logger.info(f"⬇️  Requesting ERDDAP data")
        logger.info(f"   ├── 📦 {dataset}")
        logger.info(f"   ├── 🌐 {region_id}")
        logger.info(f"   └── 🔗 URL: {url}")

        try:
            # Match curl's configuration
            async with self.session.get(
                url,
                headers=self.headers,
                timeout=self.timeout,
                ssl=True,  # SSL verification worked in curl
                raise_for_status=True
            ) as response:
                # Create output directory
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Stream to file with same chunk size as curl
                with open(output_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)
                
                size_mb = output_path.stat().st_size / 1024 / 1024
                logger.info(f"✅ Downloaded {size_mb:.1f}MB to {output_path.name}")
                return output_path

        except asyncio.TimeoutError as e:
            logger.error(f"❌ Download timed out for {dataset}")
            logger.error(f"   ├── 🕒 Timeout after {self.timeout.total}s")
            logger.error(f"   ├── 🌐 {region_id}")
            logger.error(f"   └── 🔗 {url}")
            raise

        except aiohttp.ClientError as e:
            logger.error(f"❌ Download failed for {dataset}")
            logger.error(f"   ├── 💥 {str(e)}")
            logger.error(f"   ├── 🌐 {region_id}")
            logger.error(f"   └── 🔗 {url}")
            raise
