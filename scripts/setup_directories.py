#!/usr/bin/env python3

import os
from pathlib import Path
import logging
import sys
from typing import List
from datetime import datetime, timedelta

# Add project root to Python path
root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

from config.settings import PATHS, SOURCES
from config.regions import REGIONS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def set_permissions(path: Path):
    """Set directory permissions to 777 (rwxrwxrwx) for full access."""
    try:
        os.chmod(path, 0o777)
        logger.debug(f"Set permissions for: {path}")
    except Exception as e:
        logger.error(f"Failed to set permissions for {path}: {e}")

def create_directory(path: Path):
    """Create directory and set permissions."""
    try:
        path.mkdir(parents=True, exist_ok=True)
        # Set permissions for the directory and all its parents up to the root
        current = path
        while current != root_dir:
            set_permissions(current)
            current = current.parent
    except Exception as e:
        logger.error(f"Failed to create directory {path}: {e}")
        raise

def setup_directories():
    """Create directory structure with proper permissions."""
    try:
        # Create base directories
        base_directories = [
            PATHS['STATIC_DIR'],
            PATHS['VECTOR_TILES_DIR'],
            PATHS['REGION_THUMBNAILS_DIR'],
            PATHS['API_DIR'],
            PATHS['DOWNLOADED_DATA_DIR'],
            PATHS['DATA_DIR']
        ]

        # Create and set permissions for base directories
        for directory in base_directories:
            create_directory(directory)
            logger.info(f"Created base directory: {directory}")

        # Get today's date for creating date directories
        today = datetime.now()
        dates = [today + timedelta(days=i) for i in range(-2, 3)]  # Create dirs for ±2 days

        # Create data subdirectories for each region and dataset
        for region_id in REGIONS.keys():
            for dataset_id in SOURCES.keys():
                # Create downloaded data structure
                for date in dates:
                    date_str = date.strftime('%Y%m%d')
                    
                    # Downloaded data path
                    downloaded_path = PATHS['DOWNLOADED_DATA_DIR'] / region_id / dataset_id / date_str
                    create_directory(downloaded_path)
                    logger.info(f"Created downloaded data directory: {downloaded_path}")

                    # Processed data path
                    processed_path = PATHS['DATA_DIR'] / region_id / dataset_id / date_str
                    create_directory(processed_path)
                    logger.info(f"Created processed data directory: {processed_path}")

        logger.info("Directory setup complete")
        
    except Exception as e:
        logger.error(f"Error setting up directories: {e}")
        raise

if __name__ == "__main__":
    setup_directories() 