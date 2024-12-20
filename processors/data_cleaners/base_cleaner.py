from abc import ABC, abstractmethod
import xarray as xr

class BaseDataCleaner(ABC):
    """Base class for data cleaning operations."""
    
    @abstractmethod
    def clean(self, data: xr.Dataset) -> xr.Dataset:
        """Clean the input dataset.
        
        Args:
            data: Dataset to clean
            
        Returns:
            Cleaned dataset
        """
        pass