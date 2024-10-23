from pydantic_settings import BaseSettings
from pydantic import BaseModel
from typing import Dict, List, Optional, Tuple, ClassVar
from pathlib import Path
from enum import Enum
from .regions import RegionCode, REGIONS, TimeRange

class ERDDAPConfig(BaseModel):
    """ERDDAP source configuration."""
    base_url: str = "https://coastwatch.noaa.gov/erddap/griddap"
    dataset_id: str
    variables: List[str]
    time_format: str
    regions: List[RegionCode]

class SatelliteConfig(BaseModel):
    """Direct satellite source configuration."""
    base_url: str
    prefix: str
    product: str
    measurement: str
    resolution: str
    file_format: str

class Settings(BaseSettings):
    """Application configuration."""
    
    # API Configuration
    NOAA_API_KEY: Optional[str] = None
    NOAA_BASE_URL: str = "https://coastwatch.noaa.gov/erddap/griddap"
    
    # Storage paths
    BASE_PATH: Path = Path(__file__).parent.parent.parent
    RAW_PATH: Path = BASE_PATH / "data/raw"
    PROCESSED_PATH: Path = BASE_PATH / "processed"
    TILE_PATH: Path = BASE_PATH / "tiles"
    
    # Processing settings
    ZOOM_LEVELS: list[int] = [5, 8, 10]
    TILE_SIZE: int = 256
    
    # Data sources
    SOURCES: ClassVar[Dict] = {
        "erddap": ERDDAPConfig(
            dataset_id="noaacwBLENDEDsstDNDaily",
            variables=["analysed_sst"],
            time_format="%Y-%m-%dT00:00:00Z",
            regions=[RegionCode.GULF_MEXICO, RegionCode.EAST_COAST, 
                    RegionCode.NORTHEAST, RegionCode.MID_ATLANTIC, 
                    RegionCode.SOUTH_ATLANTIC]
        ),
        "east_coast": {
            "base_url": "https://eastcoast.coastwatch.noaa.gov/data",
            "regions": [RegionCode.GULF_MEXICO, RegionCode.EAST_COAST, 
                       RegionCode.NORTHEAST, RegionCode.MID_ATLANTIC, 
                       RegionCode.SOUTH_ATLANTIC],
            "avhrr-viirs": SatelliteConfig(
                base_url="https://eastcoast.coastwatch.noaa.gov/data",
                prefix="ACSPOCW",
                product="MULTISAT",
                measurement="SST-NGT",
                resolution="750M",
                file_format="ACSPOCW_{date}_{time_range}_MULTISAT_SST-NGT_{region}_750M.nc4"
            )
        }
    }
    
    class Config:
        env_file = ".env"

settings = Settings()
