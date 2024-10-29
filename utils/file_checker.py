import xarray as xr
import logging
import numpy as np
from pathlib import Path
from typing import Optional
from datetime import datetime
from config.regions import REGIONS

logger = logging.getLogger(__name__)

def check_existing_data(path: Path, region: dict, dataset_id: str, date: datetime) -> Optional[Path]:
    """
    Check if valid data exists for the given parameters.
    
    Args:
        path: Path to check for existing data
        region: Region configuration dictionary
        dataset_id: ERDDAP dataset ID
        date: Date to check for
        
    Returns:
        Path to existing data if valid, None otherwise
    """
    try:
        if not path.exists():
            return None
            
        # Try to open and validate the dataset
        ds = xr.open_dataset(path)
        
        # Basic validation - check if dataset has expected time
        if 'time' in ds.dims:
            times = ds.time.values
            target_date = np.datetime64(date)
            if target_date not in times:
                logger.warning(f"Existing file doesn't contain data for {date}")
                return None
                
        # Additional validation could be added here
        
        return path
        
    except Exception as e:
        logger.warning(f"Error checking existing data: {str(e)}")
        return None 