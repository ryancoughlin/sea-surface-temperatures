import asyncio
from pathlib import Path
import logging
import copernicusmarine
from datetime import datetime, timedelta
from typing import Optional, Dict
from config.settings import SOURCES
from config.regions import REGIONS
from dataclasses import dataclass

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

    def _get_date_range(self, date: datetime, dataset: str) -> tuple[str, str]:
        """Get start and end dates for CMEMS query"""
        lag_days = SOURCES[dataset].get('lag_days', 0)
        query_date = date - timedelta(days=lag_days)
        
        # Format for CMEMS API (they require specific format)
        # API requires format: YYYY-MM-DDT00:00:00Z
        start_date = query_date.strftime("%Y-%m-%d")
        
        # Return same day for both start and end as per API example
        return f"{start_date}T00:00:00Z", f"{start_date}T23:59:59Z"

    async def save_data(self, date: datetime, dataset: str, region_id: str) -> Path:
        """Fetch and save CMEMS data using official toolbox"""
        try:
            logger.info(
                "Starting CMEMS data fetch\n"
                f"Dataset: {dataset}\n"
                f"Region:  {region_id}\n"
                f"Date:    {date.strftime('%Y-%m-%d')}"
            )
            
            source_config = SOURCES[dataset]
            bounds = REGIONS[region_id]['bounds']
            output_path = self.path_manager.get_data_path(date, dataset, region_id)
            
            # Create parent directories
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Use CMEMS toolbox to subset and download data
            data = copernicusmarine.subset(
                dataset_id=source_config['dataset_id'],
                variables=source_config['variables'],
                minimum_longitude=bounds[0][0],
                maximum_longitude=bounds[1][0],
                minimum_latitude=bounds[0][1],
                maximum_latitude=bounds[1][1],
                start_datetime=date.strftime('%Y-%m-%d'),
                end_datetime=date.strftime('%Y-%m-%d'),
                minimum_depth=0,
                maximum_depth=1,
                force_download=True,
                output_filename=str(output_path)
            )
            
            logger.info(
                "Successfully saved CMEMS data\n"
                f"Output:  {output_path}"
            )
            return output_path

        except Exception as e:
            logger.error(
                "Error in CMEMS data fetch\n"
                f"Dataset: {dataset}\n"
                f"Region:  {region_id}\n"
                f"Error:   {str(e)}"
            )
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