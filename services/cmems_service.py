from __future__ import annotations  # Better type hints support
import asyncio
from pathlib import Path
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Tuple, Protocol
from functools import wraps
from config.settings import SOURCES
from config.regions import REGIONS
from dataclasses import dataclass

logger = logging.getLogger(__name__)

class PathManagerProtocol(Protocol):
    """Protocol defining required path manager interface"""
    def get_data_path(self, date: datetime, dataset: str, region_id: str) -> Path:
        ...

@dataclass(frozen=True, slots=True)  # Use slots for better memory usage
class CMEMSTask:
    """Immutable task definition for CMEMS data fetching"""
    region_id: str
    dataset: str
    date: datetime

class CMEMSError(Exception):
    """Base exception for CMEMS-related errors"""
    pass

class CMEMSConfigError(CMEMSError):
    """Configuration-related errors"""
    pass

class CMEMSDownloadError(CMEMSError):
    """Download-related errors"""
    pass

def log_execution_time(func):
    """Decorator to log execution time of async functions"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = datetime.now()
        try:
            result = await func(*args, **kwargs)
            elapsed = datetime.now() - start_time
            logger.info(f"{func.__name__} completed in {elapsed.total_seconds():.2f}s")
            return result
        except Exception as e:
            elapsed = datetime.now() - start_time
            logger.error(f"{func.__name__} failed after {elapsed.total_seconds():.2f}s: {str(e)}")
            raise
    return wrapper

class CMEMSService:
    """Service for handling CMEMS data retrieval and processing"""
    
    def __init__(
        self, 
        session, 
        path_manager: PathManagerProtocol,
        max_concurrent: int = 2,
        timeout: int = 300
    ):
        self.session = session
        self.path_manager = path_manager
        self.timeout = timeout
        self._semaphore = asyncio.Semaphore(max_concurrent)

    @staticmethod
    def _standardize_date(date: datetime) -> datetime:
        """Standardize date to UTC timezone"""
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return date.astimezone(timezone.utc)

    @staticmethod
    def _format_cmems_date(date: datetime) -> Tuple[str, str]:
        """Format date range for CMEMS API"""
        date_str = date.strftime('%Y-%m-%d')
        return f"{date_str}T00:00:00Z", f"{date_str}T23:59:59Z"

    def _get_config(self, dataset: str, region_id: str) -> Tuple[dict, tuple]:
        """Get and validate configuration"""
        try:
            source_config = SOURCES[dataset]
            bounds = REGIONS[region_id]['bounds']
            return source_config, bounds
        except KeyError as e:
            raise CMEMSConfigError(f"Missing configuration for {e}")

    @log_execution_time
    async def save_data(self, date: datetime, dataset: str, region_id: str) -> Path:
        """
        Fetch and save CMEMS data using official toolbox.
        
        Args:
            date: Target date for data
            dataset: Dataset identifier
            region_id: Region identifier
            
        Returns:
            Path to saved data file
            
        Raises:
            CMEMSConfigError: If configuration is invalid
            CMEMSDownloadError: If download fails
        """
        async with self._semaphore:
            try:
                # Get and validate configuration
                source_config, bounds = self._get_config(dataset, region_id)
                
                # Process dates
                lag_days = source_config.get('lag_days', 0)
                utc_date = self._standardize_date(date)
                query_date = utc_date - timedelta(days=lag_days)
                
                logger.info(
                    "Starting CMEMS data fetch\n"
                    f"Dataset: {dataset}\n"
                    f"Region:  {region_id}\n"
                    f"Request Date: {utc_date:%Y-%m-%d}\n"
                    f"Query Date:   {query_date:%Y-%m-%d}\n"
                    f"Lag Days:     {lag_days}"
                )
                
                # Prepare output path
                output_path = self.path_manager.get_data_path(date, dataset, region_id)
                output_path.parent.mkdir(parents=True, exist_ok=True)

                # Get formatted dates for CMEMS
                start_datetime, end_datetime = self._format_cmems_date(query_date)

                try:
                    await asyncio.wait_for(
                        self._download_data(
                            source_config=source_config,
                            bounds=bounds,
                            start_datetime=start_datetime,
                            end_datetime=end_datetime,
                            output_path=output_path
                        ),
                        timeout=self.timeout
                    )
                except asyncio.TimeoutError:
                    raise CMEMSDownloadError(
                        f"Download timeout after {self.timeout} seconds"
                    )
                except Exception as e:
                    raise CMEMSDownloadError(f"Download failed: {str(e)}")
                
                logger.info(f"Successfully saved CMEMS data to {output_path}")
                return output_path

            except CMEMSError:
                raise
            except Exception as e:
                logger.error(f"Unexpected error in CMEMS data fetch: {str(e)}")
                raise CMEMSError(f"Unexpected error: {str(e)}")

    async def _download_data(
        self, 
        source_config: dict,
        bounds: tuple,
        start_datetime: str,
        end_datetime: str,
        output_path: Path
    ) -> None:
        """Handle the actual data download"""
        return await copernicusmarine.subset(
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

    async def process_dataset(self, task: CMEMSTask) -> Dict:
        """
        Process a single dataset with proper error handling.
        
        Args:
            task: CMEMSTask containing processing details
            
        Returns:
            Dictionary containing processing results
        """
        try:
            data_path = await self.save_data(
                date=task.date,
                dataset=task.dataset,
                region_id=task.region_id
            )
            
            return {
                'status': 'success',
                'path': str(data_path),
                'region': task.region_id,
                'dataset': task.dataset
            }
            
        except asyncio.CancelledError:
            logger.warning(f"Task cancelled: {task.dataset} for {task.region_id}")
            raise
        except CMEMSConfigError as e:
            logger.error(f"Configuration error: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'error_type': 'configuration',
                'region': task.region_id,
                'dataset': task.dataset
            }
        except CMEMSDownloadError as e:
            logger.error(f"Download error: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'error_type': 'download',
                'region': task.region_id,
                'dataset': task.dataset
            }
        except Exception as e:
            logger.error(f"Unexpected error processing task: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'error_type': 'unexpected',
                'region': task.region_id,
                'dataset': task.dataset 
            }