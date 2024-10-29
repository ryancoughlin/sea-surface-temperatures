from pathlib import Path
from typing import Optional
import logging
from datetime import datetime
from config.settings import SOURCES

logger = logging.getLogger(__name__)

def check_existing_data(data_dir: Path, region: dict, dataset_id: str, date: datetime) -> Optional[Path]:
    """
    Check if raw NetCDF data already exists for given parameters.
    
    Args:
        data_dir: Base data directory for downloaded files
        region: Region dictionary containing name and other properties
        dataset_id: Dataset identifier
        date: Date to check
        
    Returns:
        Path if file exists, None otherwise
    """
    # Get source configuration
    source_config = next(
        (config for _, config in SOURCES.items() 
         if config.get('dataset_id') == dataset_id),
        None
    )
    
    if not source_config:
        logger.warning(f"No configuration found for dataset {dataset_id}")
        return None

    # Match exact path construction from ERDDAPService
    output_dir = data_dir / region['name'].lower().replace(" ", "_") / source_config['name'].lower().replace(" ", "_")
    output_file = output_dir / f"{source_config['dataset_id']}_{region['name'].lower().replace(' ', '_')}_{date.strftime('%Y%m%d')}.nc"
    
    if output_file.exists():
        logger.info(f"NetCDF data already exists: {output_file}")
        return output_file
        
    return None 