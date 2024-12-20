from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict
from config.settings import SOURCES, PATHS, LAYER_TYPES, FILE_EXTENSIONS
import shutil
import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class DataFileInfo:
    """Information about a data file."""
    path: Path
    dataset: str
    region: str
    date: datetime

@dataclass
class AssetPaths:
    """Container for asset paths."""
    data: Path
    image: Path
    contours: Path
    features: Path

class PathError(Exception):
    """Base exception for path-related errors."""
    pass

class PathManager:
    """Manages data file storage operations including:
    - Local file storage for downloaded data
    - Asset paths for processed outputs
    - Cleanup of old files (>5 days)
    """
    
    def __init__(self, base_dir: Optional[Path] = None):
        self.base_dir = base_dir or Path(__file__).parent.parent
        self.data_dir = PATHS['DOWNLOADED_DATA_DIR']
        self.output_dir = PATHS['DATA_DIR']
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self.output_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise PathError(f"Failed to create directories: {e}")

    def get_data_path(self, date: datetime, dataset: str, region: str) -> Path:
        """Get path for data file"""
        try:
            # For regular datasets, get dataset_id from SOURCES
            if dataset in SOURCES:
                source_config = SOURCES[dataset]
                if source_config.get('source_type') == 'combined_view':
                    # For combined views, use the dataset name as is
                    dataset_id = dataset
                else:
                    # For regular datasets, use dataset_id if available
                    dataset_id = source_config.get('dataset_id', dataset)
            else:
                # For source datasets, use the dataset string directly as it's already a dataset_id
                dataset_id = dataset
            
            region_name = region.lower().replace(" ", "_")
            date_str = date.strftime('%Y%m%d_%H')
            return self.data_dir / f"{dataset_id}_{region_name}_{date_str}.nc"
        except Exception as e:
            raise PathError(f"Failed to construct data path: {e}")

    def get_asset_paths(self, date: datetime, dataset: str, region: str) -> AssetPaths:
        """Get paths for all assets for a given date, dataset, and region."""
        try:
            base_dir = self.output_dir / region / dataset / date.strftime('%Y%m%d')
            base_dir.mkdir(parents=True, exist_ok=True)
            
            return AssetPaths(
                data=base_dir / f"{LAYER_TYPES['DATA']}.{FILE_EXTENSIONS[LAYER_TYPES['DATA']]}",
                image=base_dir / f"{LAYER_TYPES['IMAGE']}.{FILE_EXTENSIONS[LAYER_TYPES['IMAGE']]}",
                contours=base_dir / f"{LAYER_TYPES['CONTOURS']}.{FILE_EXTENSIONS[LAYER_TYPES['CONTOURS']]}",
                features=base_dir / f"{LAYER_TYPES['FEATURES']}.{FILE_EXTENSIONS[LAYER_TYPES['FEATURES']]}"
            )
        except Exception as e:
            raise PathError(f"Failed to construct asset paths: {e}")

    def find_local_file(self, dataset: str, region: str, date: datetime) -> Optional[Path]:
        """Check if a local copy of the file exists"""
        try:
            path = self.get_data_path(date, dataset, region)
            if path.exists():
                return path
            return None
        except Exception as e:
            logger.error(f"Error finding local file: {e}")
            return None

    def store_local_copy(self, source_path: Path, dataset: str, region: str, date: datetime) -> DataFileInfo:
        """Store a local copy of downloaded data"""
        try:
            local_path = self.get_data_path(date, dataset, region)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy instead of move to preserve original
            shutil.copy2(source_path, local_path)
            return DataFileInfo(path=local_path, dataset=dataset, region=region, date=date)
        except Exception as e:
            raise PathError(f"Failed to store local copy: {e}")

    def cleanup_old_data(self, keep_days: int = 5) -> int:
        """
        Remove data older than specified number of days.
        Returns the number of files cleaned up.
        """
        cleaned_count = 0
        try:
            cutoff_date = datetime.now() - timedelta(days=keep_days)
            
            # Clean data directory
            if self.data_dir.exists():
                for file in self.data_dir.glob("*.nc"):
                    try:
                        match = re.search(r'_(\d{8})\.nc$', file.name)
                        if match:
                            file_date = datetime.strptime(match.group(1), '%Y%m%d')
                            if file_date < cutoff_date:
                                file.unlink()
                                cleaned_count += 1
                    except Exception as e:
                        logger.error(f"Error processing file {file}: {e}")
                        continue
            
            return cleaned_count
            
        except Exception as e:
            logger.error(f"Error during data cleanup: {e}")
            raise PathError(f"Failed to clean up old data: {e}")