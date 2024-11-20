from __future__ import annotations  # Better type hints support
import asyncio
from pathlib import Path
import logging
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Tuple, Protocol
from functools import wraps
from config.settings import SOURCES
from config.regions import REGIONS
from dataclasses import dataclass
import copernicusmarine
from dateutil.parser import parse as parse_date
import xarray as xr
import gc
import time

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
        timeout: int = 1800  # Increased timeout to 30 minutes
    ):
        self.session = session
        self.path_manager = path_manager
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(2)
        self.logger = logger
        
        # Configure compression settings
        self.compression_enabled = True
        self.compression_level = 5  # Balanced between size and speed

    def _get_date_range(self, date: datetime, dataset: str) -> tuple[str, str]:
        """Get start and end dates for CMEMS query"""
        lag_days = SOURCES[dataset].get('lag_days', 0)
        query_date = date - timedelta(days=lag_days)
        
        # Format for CMEMS API (they require specific format)
        start_date = query_date.strftime("%Y-%m-%d")
        
        # Return same day for both start and end
        return f"{start_date}T00:00:00", f"{start_date}T23:59:59"

    async def save_data(self, date: datetime, dataset: str, region_id: str) -> Path:
        """Fetch and save CMEMS data using official toolbox"""
        try:
            # Replace asyncio.timeout with asyncio.wait_for
            return await asyncio.wait_for(
                self._download_data(date, dataset, region_id),
                timeout=self.timeout
            )
        except asyncio.TimeoutError:
            logger.error(f"Timeout exceeded ({self.timeout}s) while downloading CMEMS data")
            raise CMEMSDownloadError(f"Download timeout after {self.timeout} seconds")
        except Exception as e:
            logger.error(f"Error in CMEMS data fetch: {str(e)}\n{traceback.format_exc()}")
            raise

    async def _download_data(self, date: datetime, dataset: str, region_id: str) -> Path:
        """Enhanced CMEMS data download with detailed logging and error handling"""
        start_time = time.time()
        
        self.logger.info("🔍 CMEMS Download Configuration:")
        self.logger.info(f"   ├── Process ID: {id(self)}")
        self.logger.info(f"   ├── Timeout: {self.timeout}s")
        self.logger.info(f"   └── Compression: Level {self.compression_level}")

        output_path = self.path_manager.get_data_path(date, dataset, region_id)
        source_config = SOURCES[dataset]
        bounds = REGIONS[region_id]['bounds']

        # Detailed parameter logging
        self.logger.info("📋 Request Parameters:")
        self.logger.info(f"   ├── Dataset ID: {source_config['dataset_id']}")
        self.logger.info(f"   ├── Variables: {source_config['variables']}")
        self.logger.info(f"   ├── Bounds: {bounds}")
        
        start_datetime, end_datetime = self._get_date_range(date, dataset)
        self.logger.info(f"   └── Time Range: {start_datetime} → {end_datetime}")

        try:
            self.logger.info("🚀 Initiating CMEMS API request...")
            
            # Track memory usage
            initial_memory = self._get_memory_usage()
            self.logger.info(f"   └── Initial Memory: {initial_memory:.2f} MB")

            data = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: copernicusmarine.subset(
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
                    output_filename=str(output_path),
                    netcdf_compression_enabled=self.compression_enabled,
                    netcdf_compression_level=self.compression_level,
                    disable_progress_bar=False,  # Enable progress tracking
                )
            )

            # Verify download success
            if not output_path.exists():
                raise CMEMSDownloadError("Download completed but file not found")

            file_size = output_path.stat().st_size
            final_memory = self._get_memory_usage()
            elapsed_time = time.time() - start_time

            # Detailed success logging
            self.logger.info("✅ Download Statistics:")
            self.logger.info(f"   ├── Time: {elapsed_time:.2f}s")
            self.logger.info(f"   ├── File Size: {file_size/1024/1024:.2f} MB")
            self.logger.info(f"   ├── Memory Delta: {final_memory - initial_memory:.2f} MB")
            self.logger.info(f"   └── Path: {output_path}")

            # Validate file content
            self._validate_netcdf(output_path)
            
            return output_path

        except Exception as e:
            elapsed_time = time.time() - start_time
            self.logger.error("❌ Download Failed:")
            self.logger.error(f"   ├── Time: {elapsed_time:.2f}s")
            self.logger.error(f"   ├── Error: {type(e).__name__}")
            self.logger.error(f"   ├── Message: {str(e)}")
            self.logger.error(f"   └── Traceback:\n{traceback.format_exc()}")
            raise

    def _validate_netcdf(self, file_path: Path) -> None:
        """Validate NetCDF file content"""
        try:
            with xr.open_dataset(file_path) as ds:
                self.logger.info("📊 File Validation:")
                self.logger.info(f"   ├── Variables: {list(ds.data_vars)}")
                self.logger.info(f"   ├── Dimensions: {dict(ds.dims)}")
                self.logger.info(f"   └── Time Range: {ds.time.values[0]} → {ds.time.values[-1]}")
        except Exception as e:
            self.logger.error(f"❌ Validation Failed: {str(e)}")
            raise CMEMSDownloadError(f"Invalid NetCDF file: {str(e)}")

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024

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