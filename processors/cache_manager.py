from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class CacheManager:
    """Manages data file caching and retrieval"""
    
    def __init__(self, base_dir: Path = Path("data")):
        self.base_dir = base_dir
        self.base_dir.mkdir(exist_ok=True)
    
    def get_cache_path(self, dataset: str, region: str, date: datetime) -> Path:
        """Get the expected cache file path"""
        date_str = date.strftime("%Y%m%d_%H")
        if "CMEMS" in dataset:
            filename = f"cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m_{region}_{date_str}.nc"
        else:
            filename = f"{dataset}_{region}_{date_str}.nc"
        return self.base_dir / filename
    
    def has_valid_cache(self, dataset: str, region: str, date: datetime, max_age_hours: int = 24) -> bool:
        """Check if there's a valid cache file for the given parameters"""
        cache_path = self.get_cache_path(dataset, region, date)
        
        if not cache_path.exists():
            return False
            
        # Check file age
        file_age = datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)
        if file_age > timedelta(hours=max_age_hours):
            logger.debug(f"Cache file too old ({file_age.total_seconds()/3600:.1f} hours)")
            return False
            
        return True
    
    def get_cached_file(self, dataset: str, region: str, date: datetime) -> Optional[Path]:
        """Get the cached file if it exists and is valid"""
        cache_path = self.get_cache_path(dataset, region, date)
        if self.has_valid_cache(dataset, region, date):
            logger.debug(f"Using cached file: {cache_path.name}")
            return cache_path
        return None
    
    def save_to_cache(self, source_path: Path, dataset: str, region: str, date: datetime) -> Path:
        """Save a file to cache"""
        cache_path = self.get_cache_path(dataset, region, date)
        
        # If source is already at cache path, just return it
        if source_path == cache_path:
            return cache_path
            
        # Copy to cache location
        cache_path.write_bytes(source_path.read_bytes())
        logger.debug(f"Saved to cache: {cache_path.name}")
        
        return cache_path
        
    def clear_old_cache(self, max_age_hours: int = 24):
        """Clear cache files older than max_age_hours"""
        now = datetime.now()
        for file in self.base_dir.glob("*.nc"):
            file_age = now - datetime.fromtimestamp(file.stat().st_mtime)
            if file_age > timedelta(hours=max_age_hours):
                file.unlink()
                logger.debug(f"Removed old cache file: {file.name}") 