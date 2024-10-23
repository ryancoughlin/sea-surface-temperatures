from pydantic_settings import BaseSettings
from pydantic import BaseModel
from typing import Dict, List, Optional, Tuple, ClassVar, TypedDict
from pathlib import Path
from enum import Enum
from .regions import RegionCode, REGIONS

class TimeRange(str, Enum):
    DAILY = "daily"
    THREE_DAY = "3day"

class ERDDAPConfig(BaseModel):
    """ERDDAP source configuration."""
    base_url: str = "https://coastwatch.noaa.gov/erddap/griddap"
    dataset_id: str
    variables: List[str]
    time_format: str
    regions: List[RegionCode]
    update_frequency: str  # e.g., "daily"
    time_lag_hours: int   # hours behind current time
    info_url: str         # metadata URL

class SatelliteConfig(BaseModel):
    """Direct satellite source configuration."""
    base_url: str
    prefix: str
    product: str
    measurement: str
    resolution: str
    file_format: str

class EastCoastConfig(BaseModel):
    """East Coast source configuration."""
    base_url: str
    regions: List[RegionCode]
    avhrr_viirs: SatelliteConfig

class TileSourceConfig(TypedDict):
    path: str
    zoom_levels: List[int]
    update_frequency: str

class TileStructure(TypedDict):
    base: Path
    sources: Dict[str, TileSourceConfig]

class Settings(BaseSettings):
    """Application configuration."""
    
    # API Configuration
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
                    RegionCode.SOUTH_ATLANTIC],
            update_frequency="daily",
            time_lag_hours=48,  # 2 days behind
            info_url="https://coastwatch.noaa.gov/erddap/info/noaacwBLENDEDsstDNDaily/index.html"
        ),
        "east_coast": EastCoastConfig(
            base_url="https://eastcoast.coastwatch.noaa.gov/data",
            regions=[RegionCode.GULF_MEXICO, RegionCode.EAST_COAST, 
                    RegionCode.NORTHEAST, RegionCode.MID_ATLANTIC, 
                    RegionCode.SOUTH_ATLANTIC],
            avhrr_viirs=SatelliteConfig(
                base_url="https://eastcoast.coastwatch.noaa.gov/data",
                prefix="ACSPOCW",
                product="MULTISAT",
                measurement="SST-NGT",
                resolution="750M",
                variable="sst",
                file_format="ACSPOCW_{date}_{time_range}_MULTISAT_SST-NGT_{region}_750M.nc4"
            )
        )
    }
    
    # Add to Settings class
    TILE_STRUCTURE: ClassVar[TileStructure] = {
        'base': BASE_PATH / "tiles",
        'sources': {
            'erddap': {
                'path': 'erddap',
                'zoom_levels': ZOOM_LEVELS,
                'update_frequency': TimeRange.DAILY,
                'time_ranges': [
                    TimeRange.DAILY,
                    TimeRange.THREE_DAY
                ]
            },
            'east_coast': {
                'path': 'east-coast',
                'zoom_levels': ZOOM_LEVELS,
                'update_frequency': TimeRange.DAILY,
                'time_ranges': [TimeRange.DAILY]
            }
        }
    }
    
    class Config:
        env_file = ".env"

settings = Settings()
