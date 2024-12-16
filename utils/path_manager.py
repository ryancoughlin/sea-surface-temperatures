from pathlib import Path
from datetime import datetime, timedelta
from typing import NamedTuple, Optional
from config.settings import SOURCES
import shutil
import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class AssetPaths:
    """Container for asset paths."""
    data: Path
    image: Path
    contours: Path
    features: Path

class PathManager:
    """Manages file system operations including:
    - Local file storage for downloaded data
    - Output directory structure for processed results
    - Cleanup of old files (>5 days)
    """
    
    BASE_DIR = Path(__file__).parent.parent

    def __init__(self):
        self.base_dir = self.BASE_DIR
        self.data_dir = self.base_dir / "data"
        self.output_dir = self.base_dir / "output"
        self.ensure_directories()

    def ensure_directories(self):
        """Ensure all required directories exist"""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_data_path(self, date: datetime, dataset: str, region: str) -> Path:
        """Get path for data file"""
        # For regular datasets, get dataset_id from SOURCES
        if dataset in SOURCES:
            dataset_id = SOURCES[dataset]['dataset_id']
        else:
            # For source datasets, use the dataset string directly as it's already a dataset_id
            dataset_id = dataset
        
        region_name = region.lower().replace(" ", "_")
        date_str = date.strftime('%Y%m%d_%H')
        return self.data_dir / f"{dataset_id}_{region_name}_{date_str}.nc"

    def get_asset_paths(self, date: datetime, dataset: str, region: str) -> AssetPaths:
        """Get paths for all assets for a given date, dataset, and region."""
        base_dir = self.output_dir / region / date.strftime('%Y%m%d') / dataset
        base_dir.mkdir(parents=True, exist_ok=True)
        
        return AssetPaths(
            data=base_dir / 'data.json',
            image=base_dir / 'image.png',
            contours=base_dir / 'contours.json',
            features=base_dir / 'features.json'
        )

    def get_metadata_path(self) -> Path:
        """Get path to the global metadata file."""
        return self.output_dir / "metadata.json"

    def find_local_file(self, dataset: str, region: str, date: datetime) -> Optional[Path]:
        """Check if a local copy of the file exists"""
        path = self.get_data_path(date, dataset, region)
        if path.exists():
            logger.info(f"Using existing local file: {path.name}")
            return path
        return None

    def store_local_copy(self, source_path: Path, dataset: str, region: str, date: datetime) -> Path:
        """Store a local copy of downloaded data"""
        local_path = self.get_data_path(date, dataset, region)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy instead of move to preserve original
        shutil.copy2(source_path, local_path)
        logger.info(f"Stored local copy at: {local_path.name}")
        return local_path

    def cleanup_old_data(self, keep_days: int = 5):
        """Remove data older than specified number of days"""
        try:
            cutoff_date = datetime.now() - timedelta(days=keep_days)
            
            # Clean data directory
            if self.data_dir.exists():
                for file in self.data_dir.glob("*.nc"):
                    match = re.search(r'_(\d{8})\.nc$', file.name)
                    if match:
                        file_date = datetime.strptime(match.group(1), '%Y%m%d')
                        if file_date < cutoff_date:
                            file.unlink()
                            logger.info(f"Removed old data file: {file}")

            # Clean output directory
            if self.output_dir.exists():
                for region_dir in self.output_dir.iterdir():
                    if not region_dir.is_dir():
                        continue
                        
                    for dataset_dir in region_dir.iterdir():
                        if not dataset_dir.is_dir():
                            continue
                            
                        for date_dir in dataset_dir.iterdir():
                            if not date_dir.is_dir():
                                continue
                                
                            try:
                                dir_date = datetime.strptime(date_dir.name, '%Y%m%d')
                                if dir_date < cutoff_date:
                                    shutil.rmtree(date_dir)
                                    logger.info(f"Removed old output directory: {date_dir}")
                            except ValueError:
                                continue
                                
            logger.info(f"Successfully cleaned up data older than {keep_days} days")
            
        except Exception as e:
            logger.error(f"Error during data cleanup: {str(e)}")
            raise