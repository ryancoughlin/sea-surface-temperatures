from pydantic_settings import BaseSettings
from pydantic import BaseModel
from typing import Dict, List, ClassVar
from pathlib import Path
from enum import Enum
from .regions import RegionCode, REGIONS

class Dataset(BaseModel):
    """Base dataset configuration"""
    name: str
    variable: str
    time_format: str
    is_night_only: bool = True
    resolution: str
    base_url: str

class ERDDAPDataset(Dataset):
    dataset_id: str
    time_lag_hours: int = 48

class FileDataset(Dataset):
    file_pattern: str

class Settings(BaseSettings):
    """Application configuration"""
    
    # Storage paths
    BASE_PATH: Path = Path(__file__).parent.parent.parent
    RAW_PATH: Path = BASE_PATH / "data/raw"
    PROCESSED_PATH: Path = BASE_PATH / "processed"
    TILE_PATH: Path = BASE_PATH / "tiles"
    
    # Processing settings
    ZOOM_LEVELS: list[int] = [5, 8, 10]
    TILE_SIZE: int = 256
    
    # Data sources
    SOURCES: ClassVar[Dict[str, Dataset]] = {
        "blended_sst": ERDDAPDataset(
            name="NOAA Blended SST",
            dataset_id="noaacwBLENDEDsstDaily",
            variable="analysed_sst",
            time_format="%Y-%m-%dT00:00:00Z",
            resolution="2km",
            base_url="https://coastwatch.noaa.gov/erddap/griddap"
        ),
        "east_coast_sst": FileDataset(
            name="East Coast SST",
            variable="sst",
            time_format="%Y%m%d",
            resolution="750m",
            base_url="https://eastcoast.coastwatch.noaa.gov/data",
            file_pattern="ACSPOCW_{date}_MULTISAT_SST-NGT_{region}_750M.nc4"
        )
    }

    class Config:
        env_file = ".env"

settings = Settings()