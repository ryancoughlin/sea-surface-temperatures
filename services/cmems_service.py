from datetime import datetime, timedelta
from pathlib import Path
import logging
import asyncio
import copernicusmarine
import os
from config.settings import SOURCES
from config.regions import REGIONS
import xarray as xr
import numpy as np

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
            variables: Optional dictionary or list of variables. If dict, will extract keys.
                     If not provided, will look up in SOURCES
        """
        try:
            output_path = self.path_manager.get_data_path(date, dataset, region)
            if output_path.exists():
                logger.info(f"[CMEMS] Using cached data for {dataset} ({region})")
                return output_path

            logger.info(f"[CMEMS] Downloading {dataset} for {region}")
            bounds = REGIONS[region]['bounds']
            
            # Get dataset configuration
            if dataset in SOURCES:
                source_config = SOURCES[dataset]
                dataset_id = source_config.get('dataset_id', dataset)
                if variables is None:
                    variables = source_config.get('variables', {})
                    
                # Handle time selection for hourly data
                time_selection = source_config.get('time_selection', {})
                if time_selection:
                    target_hour = time_selection.get('hour', 12)  # Default to noon UTC
                    window_hours = time_selection.get('window_hours', 1)
                    
                    # Adjust date to specific hour
                    start_time = date.replace(hour=target_hour, minute=0, second=0, microsecond=0)
                    end_time = start_time + timedelta(hours=window_hours)
                    
                    logger.info(f"[CMEMS] Using hourly time selection for {dataset}")
                    logger.info(f"[CMEMS] Time selection: {start_time} to {end_time}")
                else:
                    # Use full day for non-hourly datasets
                    start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
                    end_time = date.replace(hour=23, minute=59, second=59, microsecond=999999)
                    logger.info(f"[CMEMS] Using full day selection for {dataset}")
            else:
                # If dataset not in SOURCES, assume it's a direct dataset ID
                dataset_id = dataset
                if variables is None:
                    raise ValueError(f"Variables must be provided for direct dataset ID: {dataset}")
                start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_time = date.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # Handle both dict and list variables
            var_list = list(variables.keys()) if isinstance(variables, dict) else variables
            logger.info(f"[CMEMS] Dataset ID: {dataset_id}")
            logger.info(f"[CMEMS] Requesting variables: {var_list}")
            logger.info(f"[CMEMS] Time range: {start_time} to {end_time}")
            
            try:
                logger.info(f"[CMEMS] Making request for {dataset_id} with time range {start_time} to {end_time}")
                copernicusmarine.subset(
                    dataset_id=dataset_id,
                    variables=var_list,
                    minimum_longitude=bounds[0][0],
                    maximum_longitude=bounds[1][0],
                    minimum_latitude=bounds[0][1],
                    maximum_latitude=bounds[1][1],
                    start_datetime=start_time.strftime("%Y-%m-%dT%H:%M:%S"),
                    end_datetime=end_time.strftime("%Y-%m-%dT%H:%M:%S"),
                    output_filename=str(output_path),
                    force_download=True
                )
                
                if not output_path.exists():
                    raise ValueError("Download failed - no output file created")
                
                # Verify downloaded data
                with xr.open_dataset(output_path) as ds:
                    for var in var_list:
                        if var not in ds.variables:
                            raise ValueError(f"Downloaded data missing variable: {var}")
                        if np.all(np.isnan(ds[var].values)):
                            logger.warning(f"[CMEMS] Variable {var} contains all NaN values")
                            
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