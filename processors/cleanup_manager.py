from pathlib import Path
from datetime import datetime, timedelta
import logging
import shutil

from utils.path_manager import PathManager

logger = logging.getLogger(__name__)

class CleanupManager:
    """Manages cleanup of old data files to maintain a rolling window of data"""
    
    def __init__(self, path_manager: PathManager, retention_days: int = 5):
        self.path_manager = path_manager
        self.retention_days = retention_days
        
    def cleanup_all(self):
        """Perform one-time cleanup of all data files before pipeline run"""
        logger.info(f"🧹 Starting pre-pipeline cleanup (keeping {self.retention_days} days of data)")
        
        # Clean data directory (includes downloaded and cached files)
        self._cleanup_data_directory()
        
        # Clean processed outputs
        self._cleanup_processed_outputs()
        
        logger.info("   └── ✅ Pre-pipeline cleanup completed")
        
    def _cleanup_data_directory(self):
        """Clean up all data files older than retention period"""
        data_dir = self.path_manager.data_dir
        if not data_dir.exists():
            return
            
        logger.info("   ├── 🗑️  Cleaning data directory")
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        
        for data_file in data_dir.glob("**/*"):
            if not data_file.is_file():
                continue
                
            try:
                mtime = datetime.fromtimestamp(data_file.stat().st_mtime)
                if mtime < cutoff_date:
                    data_file.unlink()
                    logger.debug(f"      Removed old file: {data_file.name}")
            except Exception as e:
                logger.error(f"      Error cleaning {data_file}: {str(e)}")
                
    def _cleanup_processed_outputs(self):
        """Clean up processed output files older than retention period"""
        logger.info("   ├── 🗑️  Cleaning processed outputs")
        
        output_dir = self.path_manager.output_dir
        if output_dir.exists():
            self._cleanup_directory_by_date_structure(output_dir)
            
    def _cleanup_directory_by_date_structure(self, directory: Path):
        """Clean up directories that use date-based structure (YYYY/MM/DD)"""
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        
        for year_dir in directory.glob("*"):
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
                
            for month_dir in year_dir.glob("*"):
                if not month_dir.is_dir() or not month_dir.name.isdigit():
                    continue
                    
                for day_dir in month_dir.glob("*"):
                    if not day_dir.is_dir() or not day_dir.name.isdigit():
                        continue
                        
                    try:
                        dir_date = datetime.strptime(
                            f"{year_dir.name}{month_dir.name}{day_dir.name}",
                            "%Y%m%d"
                        )
                        
                        if dir_date < cutoff_date:
                            shutil.rmtree(day_dir)
                            logger.debug(f"      Removed old data: {day_dir}")
                            
                    except Exception as e:
                        logger.error(f"      Error cleaning {day_dir}: {str(e)}")
                        
                # Remove empty month directories
                if not any(month_dir.iterdir()):
                    month_dir.rmdir()
                    
            # Remove empty year directories
            if not any(year_dir.iterdir()):
                year_dir.rmdir() 