from abc import ABC, abstractmethod
import xarray as xr
from pathlib import Path

class BaseDataCleaner(ABC):
    """Base class for dataset-specific data cleaning."""
    
    @abstractmethod
    def clean(self, data: xr.DataArray) -> xr.DataArray:
        """Clean the dataset according to dataset-specific rules."""
        pass 