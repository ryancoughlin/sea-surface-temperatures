import asyncio
from pathlib import Path
import logging
import copernicusmarine
from datetime import datetime, timedelta
from typing import Optional, Dict
from config.settings import SOURCES
from config.regions import REGIONS
from dataclasses import dataclass
from utils.dates import DateFormatter
from datetime import timezone

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class CMEMSTask:
    region_id: str
    dataset: str
    date: datetime

class CMEMSService:
    def __init__(self, session, path_manager, timeout: int = 300):
        self.session = session
        self.path_manager = path_manager
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(2)
        self.date_formatter = DateFormatter()

    async def save_data(self, date: datetime, dataset: str, region_id: str) -> Path:
        """Fetch and save CMEMS data using official toolbox"""
        try:
            # Convert to UTC if not already
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            utc_date = date.astimezone(timezone.utc)
            
            logger.info(
                "Starting CMEMS data fetch\n"
                f"Dataset: {dataset}\n"
                f"Region:  {region_id}\n"
                f"UTC Date: {utc_date.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            )
            
            source_config = SOURCES[dataset]
            bounds = REGIONS[region_id]['bounds']
            output_path = self.path_manager.get_data_path(date, dataset, region_id)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Format dates according to CMEMS requirements
            start_datetime = f"{utc_date.strftime('%Y-%m-%d')}T00:00:00Z"
            end_datetime = f"{utc_date.strftime('%Y-%m-%d')}T23:59:59Z"

            # Use CMEMS toolbox with proper date formatting
            data = copernicusmarine.subset(
                dataset_id=source_config['dataset_id'],
                variables=source_config['variables'],
                minimum_longitude=bounds[0][0],
                maximum_longitude=bounds[1][0],
                minimum_latitude=bounds[0][1],
                maximum_latitude=bounds[1][1],
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                minimum_depth=0,
                maximum_depth=1,
                force_download=True,
                output_filename=str(output_path)
            )
            
            logger.info(f"Successfully saved CMEMS data to {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Error in CMEMS data fetch: {str(e)}")
            raise

    async def process_dataset(self, task: CMEMSTask) -> Dict:
        """Process a single dataset"""
        try:
            data_path = await self.save_data(
                date=task.date,
                dataset=task.dataset,
                region_id=task.region_id
            )
            
            if data_path:
                return {
                    'status': 'success',
                    'path': str(data_path),
                    'region': task.region_id,
                    'dataset': task.dataset
                }
            
            return {
                'status': 'error',
                'error': 'Failed to download data',
                'region': task.region_id,
                'dataset': task.dataset
            }
            
        except asyncio.CancelledError:
            logger.warning(f"Task cancelled: {task.dataset} for {task.region_id}")
            raise
        except Exception as e:
            logger.error(f"Error processing task: {str(e)}")
            return {'status': 'error', 'error': str(e)}