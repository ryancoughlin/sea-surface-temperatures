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
        
    async def save_data(self, date: datetime, dataset: str, region: str, variables: dict = None) -> Path:
        """
        Download data from CMEMS service
        
        Args:
            date: The date to download data for
            dataset: The dataset ID or name
            region: The region to download data for
            variables: Optional dictionary of variables. If not provided, will look up in SOURCES
        """
        logger.info(f"📥 CMEMS Download:")
        logger.info(f"   └── Dataset: {dataset}")
        logger.info(f"   └── Region: {region}")
        
        try:
            # Get the proper path for the download
            output_path = self.path_manager.get_data_path(date, dataset, region)
            if output_path.exists():
                logger.info("   └── ♻️  Using existing file")
                return output_path

            logger.info("   └── 🔄 Starting download request...")
            bounds = REGIONS[region]['bounds']
            
            # Get variables either from passed dict or SOURCES
            source_config = SOURCES[dataset]
            if variables is None:
                variables = source_config['variables']
            
            # Use dataset_id from config if available, otherwise use dataset name
            dataset_id = source_config.get('dataset_id', dataset)
            
            logger.info(f"   └── Download parameters:")
            logger.info(f"      └── Dataset ID: {dataset_id}")
            logger.info(f"      └── Variables: {list(variables.keys())}")
            logger.info(f"      └── Bounds: {bounds}")
            logger.info(f"      └── Date: {date}")
            logger.info(f"      └── Output path: {output_path}")
            
            try:
                copernicusmarine.subset(
                    dataset_id=dataset_id,
                    variables=list(variables.keys()),
                    minimum_longitude=bounds[0][0],
                    maximum_longitude=bounds[1][0],
                    minimum_latitude=bounds[0][1],
                    maximum_latitude=bounds[1][1],
                    start_datetime=date.strftime("%Y-%m-%dT00:00:00"),
                    end_datetime=date.strftime("%Y-%m-%dT23:59:59"),
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
                
        except Exception as e:
            logger.error(f"   └── ⚠️  Error getting local file path: {str(e)}")
            raise