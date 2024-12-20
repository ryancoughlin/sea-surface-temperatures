from datetime import datetime, timedelta
from pathlib import Path
import logging
import asyncio
import copernicusmarine
import os
from config.settings import SOURCES, PATHS
from config.regions import REGIONS
import xarray as xr
import numpy as np

logger = logging.getLogger(__name__)

class CMEMSService:
    def __init__(self, session, path_manager):
        self.session = session
        self.path_manager = path_manager
        self.download_dir = PATHS['DOWNLOADED_DATA_DIR']
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
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
            output_path = self.download_dir / region / dataset / date.strftime('%Y%m%d') / 'raw.nc'
            if output_path.exists():
                return output_path

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
                else:
                    # Use full day for non-hourly datasets
                    start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
                    end_time = date.replace(hour=23, minute=59, second=59, microsecond=999999)
            else:
                # If dataset not in SOURCES, assume it's a direct dataset ID
                dataset_id = dataset
                if variables is None:
                    raise ValueError(f"Variables must be provided for direct dataset ID: {dataset}")
                start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_time = date.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # Handle both dict and list variables
            var_list = list(variables.keys()) if isinstance(variables, dict) else variables
            
            try:
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
                
                # Verify downloaded data with decode_times=False
                with xr.open_dataset(output_path, decode_times=False) as ds:
                    for var in var_list:
                        if var not in ds.variables:
                            raise ValueError(f"Downloaded data missing variable: {var}")
                        if np.all(np.isnan(ds[var].values)):
                            logger.warning(f"Variable {var} contains all NaN values")
                            
                return output_path

            except Exception as e:
                logger.error(f"Download failed for {dataset}: {str(e)}")
                if output_path.exists():
                    output_path.unlink()
                raise
                
        except Exception as e:
            logger.error(f"Failed to process {dataset}: {str(e)}")
            raise