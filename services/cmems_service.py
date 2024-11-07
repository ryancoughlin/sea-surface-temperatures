import logging
from pathlib import Path
from datetime import datetime
from typing import Dict
import copernicusmarine
from config.settings import SOURCES
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class CMEMSService:
    """Service for fetching data from Copernicus Marine using their official toolbox"""
    
    def __init__(self, session, path_manager):
        self.path_manager = path_manager

    async def save_data(self, date: datetime, dataset: str, region_id: str) -> Path:
        """Fetch and save CMEMS data using official toolbox"""
        try:
            logger.info(f"Starting CMEMS data fetch for dataset: {dataset}, region: {region_id}")
            
            source_config = SOURCES[dataset]
            region = REGIONS[region_id]
            bounds = region['bounds']
            
            output_path = self.path_manager.get_data_path(
                date=date,
                dataset=dataset,
                region=region_id
            )
            
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
            
            logger.info(f"Successfully saved CMEMS data to {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Error in CMEMS data fetch: {str(e)}")
            raise