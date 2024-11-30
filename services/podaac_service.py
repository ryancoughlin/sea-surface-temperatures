from datetime import datetime, time, timedelta
import logging
from pathlib import Path
import asyncio
from typing import Optional, List
import subprocess
import re

from config.settings import SOURCES
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class PodaacService:
    """Service for handling PODAAC data access using podaac-data-downloader"""
    
    def __init__(self, session, path_manager):
        self.path_manager = path_manager
        
    def _extract_datetime_from_filename(self, filename: str) -> Optional[datetime]:
        """Extract datetime from GOES16 SST filename format."""
        # Expected format: YYYYMMDDHHMMSS-OSISAF-L3C_GHRSST...
        match = re.match(r'(\d{8})(\d{6})', filename)
        if match:
            date_str, time_str = match.groups()
            try:
                return datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
            except ValueError:
                return None
        return None

    def _calculate_time_window(self) -> tuple[str, str]:
        """Calculate time window for 6 most recent hourly files."""
        now = datetime.utcnow()
        
        # For hourly data, we need the last 6 hours
        # Start from 6 hours ago to ensure we get 6 files
        start_time = now - timedelta(hours=6)
        
        # Format times in ISO format
        start_iso = start_time.strftime('%Y-%m-%dT%H:00:00Z')  # Round to hour start
        end_iso = now.strftime('%Y-%m-%dT%H:59:59Z')  # Include the full current hour
        
        logger.info(f"Calculated hourly time window: {start_iso} to {end_iso}")
        return start_iso, end_iso

    async def save_data(self, date: datetime, dataset: str, region: str) -> List[Path]:
        """Download PODAAC data for optimal time window and region using CLI tool"""
        try:
            # Create directory with UTC date
            today = datetime.utcnow().date()
            download_dir = self.path_manager.data_dir / dataset / today.strftime('%Y%m%d')
            download_dir.mkdir(parents=True, exist_ok=True)
            
            # Calculate optimal time window for ~6 files
            start_iso, end_iso = self._calculate_time_window()
            logger.info(f"Using optimized UTC time window: {start_iso} to {end_iso}")
            
            # Get region bounds
            bounds = REGIONS[region]['bounds']
            lon_min, lat_min = bounds[0]
            lon_max, lat_max = bounds[1]
            
            # Format for PODAAC: "W Longitude,S Latitude,E Longitude,N Latitude"
            bbox = f"-{abs(lon_max)},{lat_min},-{abs(lon_min)},{lat_max}"
            
            # Construct command
            cmd = [
                'podaac-data-downloader',
                '-c', SOURCES[dataset]['dataset_id'],
                '-d', str(download_dir),
                '--start-date', start_iso,
                '--end-date', end_iso,
                f'-b={bbox}'
            ]
            
            logger.info(f"Downloading PODAAC data with optimized time window")
            logger.info(f"Command: {' '.join(cmd)}")
            
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
            
            # Get all downloaded NetCDF files and sort by timestamp
            downloaded_files = list(download_dir.rglob("*.nc"))
            if not downloaded_files:
                logger.error("No files downloaded")
                return []
            
            # Sort files by timestamp (most recent first)
            sorted_files = sorted(
                downloaded_files,
                key=lambda x: self._extract_datetime_from_filename(x.name) or datetime.min,
                reverse=True
            )
            
            # Take only the 6 most recent if we got more
            if len(sorted_files) > 6:
                recent_files = sorted_files[:6]
                # Remove any extra files
                for file in sorted_files[6:]:
                    try:
                        file.unlink()
                        logger.info(f"Removed extra file: {file.name}")
                    except Exception as e:
                        logger.warning(f"Could not remove file {file.name}: {str(e)}")
            else:
                recent_files = sorted_files
            
            logger.info(f"Retained {len(recent_files)} most recent files")
            return recent_files
            
        except Exception as e:
            logger.error(f"Error in PODAAC download: {str(e)}")
            raise 