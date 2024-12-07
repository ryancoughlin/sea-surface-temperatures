from typing import Dict, Any, Optional
from pathlib import Path

class ProcessingResult:
    """Represents the result of a processing operation"""
    
    def __init__(self,
                 status: str,
                 dataset: str,
                 region: str,
                 paths: Optional[Dict[str, str]] = None,
                 error: Optional[str] = None):
        self.status = status
        self.dataset = dataset
        self.region = region
        self.paths = paths or {}
        self.error = error
        
    @property
    def is_success(self) -> bool:
        """Check if processing was successful"""
        return self.status == 'success'
    
    @classmethod
    def success(cls, dataset: str, region: str, paths: Dict[str, str]) -> 'ProcessingResult':
        """Create a successful result"""
        return cls(
            status='success',
            dataset=dataset,
            region=region,
            paths=paths
        )
    
    @classmethod
    def error(cls, dataset: str, region: str, error: str) -> 'ProcessingResult':
        """Create an error result"""
        return cls(
            status='error',
            dataset=dataset,
            region=region,
            error=error
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for serialization"""
        result = {
            'status': self.status,
            'dataset': self.dataset,
            'region': self.region
        }
        
        if self.paths:
            result['paths'] = self.paths
        if self.error:
            result['error'] = self.error
            
        return result 