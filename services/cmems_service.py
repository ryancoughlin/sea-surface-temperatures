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
        try:
            output_path = self.path_manager.get_data_path(date, dataset, region)
            if output_path.exists():
                logger.info(f"[CMEMS] Using cached data for {dataset} ({region})")
                return output_path

            logger.info(f"[CMEMS] Downloading {dataset} for {region}")
            bounds = REGIONS[region]['bounds']
            
            source_config = SOURCES[dataset]
            if variables is None:
                variables = source_config['variables']
            
            dataset_id = source_config.get('dataset_id', dataset)
            
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
                
                if not output_path.exists():
                    raise ProcessingError("download", "Download failed - no output file created", 
                                        {"path": str(output_path)})
                                        
                logger.info(f"[CMEMS] Successfully downloaded {dataset} ({output_path.stat().st_size / 1024 / 1024:.1f}MB)")
                return output_path
                
            except Exception as e:
                logger.error(f"[CMEMS] Download failed for {dataset}: {str(e)}")
                if output_path.exists():
                    output_path.unlink()
                raise
                
        except Exception as e:
            logger.error(f"[CMEMS] Failed to process {dataset}: {str(e)}")
            raise