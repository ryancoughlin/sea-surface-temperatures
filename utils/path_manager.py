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
    metadata: Path

class PathManager:
    BASE_DIR = Path(__file__).parent.parent

    def __init__(self):
        self.base_dir = self.BASE_DIR
        self.data_dir = self.base_dir / "data"
        self.output_dir = self.base_dir / "output"

    def ensure_directories(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_data_path(self, date: datetime, dataset: str, region: str) -> Path:
        # Get the dataset_id from SOURCES config
        dataset_id = SOURCES[dataset]['dataset_id']
        region_name = region.lower().replace(" ", "_")
        date_str = date.strftime('%Y%m%d_%H%M%S')
        return self.data_dir / f"{dataset_id}_{region_name}_{date_str}.nc"

    def get_asset_paths(self, date: datetime, dataset: str, region: str) -> AssetPaths:
        date_str = date.strftime('%Y%m%d/%H%M')
        dataset_dir = self.output_dir / region / dataset / date_str
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        return AssetPaths(
            image=dataset_dir / "image.png",
            data=dataset_dir / "data.json",
            contours=dataset_dir / "contours.json" if "contours" in SOURCES[dataset]["supportedLayers"] else None,
            metadata=dataset_dir / "metadata.json"
        )

    def cleanup_old_data(self, keep_days: int = 5):
        """Remove data older than specified number of days"""
        try:
            cutoff_date = datetime.now() - timedelta(days=keep_days)
            
            # Clean data directory
            if self.data_dir.exists():
                for file in self.data_dir.glob("*.nc"):
                    # Extract date from filename (format: dataset_region_YYYYMMDD.nc)
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