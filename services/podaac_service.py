from datetime import datetime, time, timedelta
import logging
from pathlib import Path
import asyncio
from typing import Optional, List
import subprocess

from config.settings import SOURCES
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class PodaacService:
    """Service for handling PODAAC data access using podaac-data-downloader"""
    
    def __init__(self, session, path_manager):
        self.path_manager = path_manager
        
    async def save_data(self, date: datetime, dataset: str, region: str) -> List[Path]:
        """Download PODAAC data for date range and region using CLI tool"""
        try:
            # Use today's date and format in ISO
            today = datetime.utcnow().date()
            start_iso = f"{today.isoformat()}T00:00:00Z"
            end_iso = f"{today.isoformat()}T23:59:59Z"
            
            download_dir = self.path_manager.data_dir / dataset / today.strftime('%Y%m%d')
            download_dir.mkdir(parents=True, exist_ok=True)
            
            # Get region bounds
            bounds = REGIONS[region]['bounds']
            lon_min, lat_min = bounds[0]
            lon_max, lat_max = bounds[1]
            
            # Format for PODAAC: "W Longitude,S Latitude,E Longitude,N Latitude"
            bbox = f"-{abs(lon_max)},{lat_min},-{abs(lon_min)},{lat_max}"
            
            # Construct command exactly matching working example
            cmd = [
                'podaac-data-downloader',
                '-c', SOURCES[dataset]['dataset_id'],
                '-d', str(download_dir),
                '--start-date', start_iso,
                '--end-date', end_iso,
                f'-b={bbox}'
            ]
            
            logger.info(f"Downloading PODAAC data for {today}")
            logger.info(f"Command: {' '.join(cmd)}")
            
            # Run downloader in a subprocess
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"Download failed: {stderr.decode()}")
                return []
                
            logger.info(f"Download completed: {stdout.decode()}")
            
            # Get all downloaded NetCDF files
            downloaded_files = list(download_dir.rglob("*.nc"))
            if not downloaded_files:
                logger.error("No files downloaded")
                return []
                
            logger.info(f"Downloaded {len(downloaded_files)} files")
            return downloaded_files
            
        except Exception as e:
            logger.error(f"Error downloading PODAAC data: {str(e)}")
            return [] 