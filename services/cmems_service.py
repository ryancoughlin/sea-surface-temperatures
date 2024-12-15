from datetime import datetime, timedelta
from pathlib import Path
import logging
import asyncio
import copernicusmarine
import os
from config.settings import SOURCES
from config.regions import REGIONS
from processors.cache_manager import CacheManager

logger = logging.getLogger(__name__)

class CMEMSService:
    def __init__(self, session, path_manager):
        self.session = session
        self.path_manager = path_manager
        self.cache_manager = CacheManager(path_manager.data_dir)
        
    async def save_data(self, date: datetime, dataset: str, region: str) -> Path:
        logger.info(f"📥 CMEMS Download:")
        logger.info(f"   └── Dataset: {dataset}")
        logger.info(f"   └── Region: {region}")
        
        # Check cache first using cache manager
        cached_file = self.cache_manager.get_cached_file(dataset, region, date)
        if cached_file:
            logger.info("   └── ♻️  Using cached data")
            return cached_file
            
        logger.info("   └── 🔄 Starting download request...")
        config = SOURCES[dataset]
        bounds = REGIONS[region]['bounds']
        
        # Adjust date for lag days
        lag_days = config.get('lag_days', 0)
        adjusted_date = date - timedelta(days=lag_days)
        
        try:
            # Get the proper cache path for the download
            output_path = self.cache_manager.get_cache_path(dataset, region, date)
            
            copernicusmarine.subset(
                dataset_id=config['dataset_id'],
                variables=config['variables'],
                minimum_longitude=bounds[0][0],
                maximum_longitude=bounds[1][0],
                minimum_latitude=bounds[0][1],
                maximum_latitude=bounds[1][1],
                start_datetime=adjusted_date.strftime("%Y-%m-%dT00:00:00"),
                end_datetime=adjusted_date.strftime("%Y-%m-%dT23:59:59"),
                output_filename=str(output_path),
               force_download=True
            )
            
            # Check if download completed
            if not output_path.exists():
                raise ProcessingError("download", "Download failed - no output file created", 
                                    {"path": str(output_path)})
                                    
            logger.info("   └── ✅ Download complete")
            logger.info(f"      └── File size: {output_path.stat().st_size / 1024 / 1024:.2f} MB")
            return output_path
            
        except Exception as e:
            logger.error(f"   └── 💥 Download failed: {str(e)}")
            logger.error(f"   └── Error type: {type(e).__name__}")
            if output_path.exists():
                output_path.unlink()
            raise