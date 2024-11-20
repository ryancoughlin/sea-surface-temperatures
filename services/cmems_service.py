from datetime import datetime, timedelta
from pathlib import Path
import logging
import asyncio
import copernicusmarine
import os
from config.settings import SOURCES
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class CMEMSService:
    def __init__(self, session, path_manager):
        self.session = session
        self.path_manager = path_manager
        
    def _setup_auth(self):
        """Setup CMEMS authentication"""
        try:
            # First try: Check if already logged in
            logger.info("🔑 Checking CMEMS authentication...")
            
            # Try to login with existing credentials file
            copernicusmarine.login(
                skip_if_user_logged_in=True  # Skip if already authenticated
            )
            logger.info("   └── ✅ Using existing credentials")
            return True
            
        except Exception as auth_error:
            logger.warning("   └── ⚠️  No existing credentials found")
            
            # Second try: Check environment variables
            username = os.getenv('COPERNICUS_MARINE_SERVICE_USERNAME')
            password = os.getenv('COPERNICUS_MARINE_SERVICE_PASSWORD')
            
            if username and password:
                try:
                    logger.info("   └── 🔄 Logging in with environment credentials")
                    copernicusmarine.login(
                        username=username,
                        password=password,
                        overwrite_configuration_file=True
                    )
                    logger.info("   └── ✅ Login successful")
                    return True
                except Exception as e:
                    logger.error("   └── ❌ Login failed with environment credentials")
                    logger.error(f"   └── Error: {str(e)}")
            else:
                logger.error("   └── ❌ No credentials found")
                logger.error("   └── Please set environment variables:")
                logger.error("   └── COPERNICUS_MARINE_SERVICE_USERNAME")
                logger.error("   └── COPERNICUS_MARINE_SERVICE_PASSWORD")
            
            return False
            
    async def save_data(self, date: datetime, dataset: str, region: str) -> Path:
        logger.info(f"📥 CMEMS Download:")
        logger.info(f"   └── Dataset: {dataset}")
        logger.info(f"   └── Region: {region}")
        
        # Verify authentication first
        if not self._setup_auth():
            raise ProcessingError("auth", "CMEMS authentication failed", 
                                {"dataset": dataset, "region": region})
        
        output_path = self.path_manager.get_data_path(date, dataset, region)
        if output_path.exists():
            logger.info("   └── ♻️  Using cached data")
            return output_path
            
        logger.info("   └── 🔄 Starting download request...")
        config = SOURCES[dataset]
        bounds = REGIONS[region]['bounds']
        
        # Adjust date for lag days
        lag_days = config.get('lag_days', 0)
        adjusted_date = date - timedelta(days=lag_days)
        
        try:
            # Log the exact request parameters
            logger.info("   └── 📋 Request parameters:")
            logger.info(f"      └── Dataset ID: {config['dataset_id']}")
            logger.info(f"      └── Variables: {config['variables']}")
            logger.info(f"      └── Time range: {adjusted_date.strftime('%Y-%m-%dT%H:%M:%S')}")
            logger.info(f"      └── Output: {output_path}")
            
            # Make the request
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