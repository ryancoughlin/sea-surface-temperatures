from datetime import datetime, timedelta
from pathlib import Path
import logging
import aiohttp
import asyncio
from config.settings import SOURCES
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class ERDDAPService:
    BASE_URL = "https://coastwatch.noaa.gov/erddap/griddap"

    def __init__(self, session: aiohttp.ClientSession, path_manager):
        self.session = session
        self.path_manager = path_manager
        
        self.timeout = aiohttp.ClientTimeout(
            total=300,     # 5 minutes total
            connect=30,    # 30s connect timeout
            sock_read=30   # 30s read timeout
        )
        self.headers = {
            'User-Agent': 'curl/8.7.1',
            'Accept': '*/*'
        }
        self.max_retries = 3
        self.retry_delay = 2  # seconds

    def build_url(self, date: datetime, dataset: str, region: str) -> str:
        """Build ERDDAP request URL"""
        config = SOURCES[dataset]
        bounds = REGIONS[region]['bounds']
        
        # Adjust date for lag days
        lag_days = config.get('lag_days', 0)
        adjusted_date = date - timedelta(days=lag_days)
        formatted_date = adjusted_date.strftime("%Y-%m-%dT00:00:00Z")
        
        base = f"{self.BASE_URL}/{config['dataset_id']}.nc?"
        var_parts = []
        
        for var in config.get('variables', []):
            constraints = [
                f"%5B({formatted_date}):1:({formatted_date})%5D"
            ]
            
            # Add altitude constraint only for datasets that require it
            if dataset == "chlorophyll_oci":
                constraints.append(f"%5B(0.0):1:(0.0)%5D")
                
            # Add lat/lon constraints
            constraints.extend([
                f"%5B({bounds[0][1]}):1:({bounds[1][1]})%5D",
                f"%5B({bounds[0][0]}):1:({bounds[1][0]})%5D"
            ])
            
            var_parts.append(f"{var}{''.join(constraints)}")
            
        return base + ','.join(var_parts)

    async def save_data(self, date: datetime, dataset: str, region: str) -> Path:
        try:
            output_path = self.path_manager.get_data_path(date, dataset, region)
            if output_path.exists():
                logger.info(f"[ERDDAP] Using cached data for {dataset} ({region})")
                return output_path

            for attempt in range(self.max_retries):
                try:
                    if attempt > 0:
                        logger.info(f"[ERDDAP] Retry {attempt + 1}/{self.max_retries} for {dataset}")
                    else:
                        logger.info(f"[ERDDAP] Downloading {dataset} for {region}")
                        
                    url = self.build_url(date, dataset, region)

                    async with self.session.get(
                        url,
                        headers=self.headers,
                        timeout=self.timeout,
                        ssl=True
                    ) as response:
                        response.raise_for_status()
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        with open(output_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(8192):
                                f.write(chunk)
                            
                    logger.info(f"[ERDDAP] Successfully downloaded {dataset} ({output_path.stat().st_size / 1024 / 1024:.1f}MB)")
                    return output_path
                except Exception as e:
                    if attempt == self.max_retries - 1:
                        logger.error(f"[ERDDAP] All download attempts failed for {dataset}: {str(e)}")
                        raise
                    await asyncio.sleep(self.retry_delay)
        except Exception as e:
            logger.error(f"[ERDDAP] Failed to process {dataset}: {str(e)}")
            raise
