from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DataService(ABC):
    """Base class for data services"""
    def __init__(self, session, path_manager):
        self.session = session
        self.path_manager = path_manager
        
    async def get_data(self, date: datetime, dataset: str, region: str) -> Path:
        """Get data from cache or source"""
        cache_path = self.path_manager.get_data_path(date, dataset, region)
        if cache_path.exists():
            logger.info(f"Using cached data: {cache_path.name}")
            return cache_path
        return await self.fetch(date, dataset, region)
    
    @abstractmethod
    async def fetch(self, date: datetime, dataset: str, region: str) -> Path:
        """Fetch data from source"""
        pass