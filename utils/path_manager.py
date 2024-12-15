from pathlib import Path
from datetime import datetime, timedelta
from typing import NamedTuple, Optional
from config.settings import SOURCES
import re
import shutil
import logging

logger = logging.getLogger(__name__)

class AssetPaths(NamedTuple):
    image: Path
    data: Path
    contours: Optional[Path]

class PathManager:
    """Manages all data operations: paths, caching, and cleanup"""
    
    BASE_DIR = Path(__file__).parent.parent

    def __init__(self):
        self.base_dir = self.BASE_DIR
        self.data_dir = self.base_dir / "data"
        self.output_dir = self.base_dir / "output"
        self.downloaded_data_dir = self.base_dir / "downloaded_data"
        self.ensure_directories()

    def ensure_directories(self):
        """Ensure all required directories exist"""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.downloaded_data_dir.mkdir(parents=True, exist_ok=True)

    def get_data_path(self, date: datetime, dataset: str, region: str) -> Path:
        """Get path for downloaded/cached data file"""
        dataset_id = SOURCES[dataset]['dataset_id']
        region_name = region.lower().replace(" ", "_")
        date_str = date.strftime('%Y%m%d_%H')
        
        # First check for exact match
        base_path = self.data_dir / f"{dataset_id}_{region_name}_{date_str}.nc"
        if base_path.exists():
            logger.info(f"Found exact cache match at {base_path}")
            return base_path
            
        # Then check for files with timestamps
        pattern = f"{dataset_id}_{region_name}_{date_str}*.nc"
        matches = list(self.data_dir.glob(pattern))
        if matches:
            logger.info(f"Found cache match at {matches[0]}")
            return matches[0]
            
        # If no existing file found, return base path for new file
        return base_path

    def get_asset_paths(self, date: datetime, dataset: str, region: str) -> AssetPaths:
        """Get asset paths for the given dataset."""
        date_str = date.strftime('%Y%m%d')
        dataset_dir = self.output_dir / region / dataset / date_str
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        return AssetPaths(
            image=dataset_dir / "image.png",
            data=dataset_dir / "data.json",
            contours=dataset_dir / "contours.json" if "contours" in SOURCES[dataset]["supportedLayers"] else None
        )

    def get_metadata_path(self) -> Path:
        """Get path to the global metadata file."""
        return self.output_dir / "metadata.json"

    def get_cached_file(self, dataset: str, region: str, date: datetime) -> Optional[Path]:
        """Get cached file if it exists"""
        path = self.get_data_path(date, dataset, region)
        if path.exists():
            logger.info(f"Using cached file: {path.name}")
            return path
        return None

    def save_to_cache(self, source_path: Path, dataset: str, region: str, date: datetime) -> Path:
        """Save downloaded data to cache"""
        cache_path = self.get_data_path(date, dataset, region)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy instead of move to preserve original
        shutil.copy2(source_path, cache_path)
        logger.info(f"Saved to cache: {cache_path.name}")
        return cache_path

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

    def clear_cache(self):
        """Clear all cached files"""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)
            self.data_dir.mkdir(parents=True)
            logger.info("Cache cleared")