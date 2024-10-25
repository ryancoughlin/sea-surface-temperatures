from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict

class BaseService(ABC):
    @abstractmethod
    async def download(self, date: datetime, dataset: Dict, region: Dict, output_path: Path) -> Path:
        """Download data and return path to downloaded file."""
        pass
