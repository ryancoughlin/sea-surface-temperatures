from pydantic_settings import BaseSettings
from pydantic import BaseModel
from typing import Dict, List, Optional
from pathlib import Path
from enum import Enum

class TimeRange(str, Enum):
    DAILY = "daily"
    THREE_DAY = "3day"
    SEVEN_DAY = "7day"
    MONTHLY = "monthly"
    SEASONAL = "seasonal"
    ANNUAL = "annual"

class Region(str, Enum):
    GULF_MEXICO = "gm"
    EAST_COAST = "ec"
    NORTHEAST = "ne"
    MID_ATLANTIC = "ma"
    SOUTH_ATLANTIC = "sa"

class Product(str, Enum):
    SST = "sst"
    CHLOROPHYLL = "chlor"
    TRUE_COLOR = "true-color"

class SatelliteConfig(BaseModel):
    """Configuration for a specific satellite product."""
    prefix: str
    product: str
    measurement: str
    resolution: str
    file_format: str

class SourceConfig(BaseModel):
    """Configuration for a data source."""
    base_url: str
    satellites: Dict[str, SatelliteConfig]
    regions: List[Region]
    time_ranges: List[TimeRange]

class Settings(BaseSettings):
    """Application configuration."""
    
    # API Configuration
    NOAA_API_KEY: str
    NOAA_BASE_URL: str = "https://coastwatch.noaa.gov/erddap/griddap"
    
    # Storage paths
    BASE_PATH: Path = Path("data")
    RAW_PATH: Path = BASE_PATH / "raw"
    PROCESSED_PATH: Path = BASE_PATH / "processed"
    TILE_PATH: Path = BASE_PATH / "tiles"
    
    # Processing settings
    ZOOM_LEVELS: List[int] = [5, 8, 10]
    TILE_SIZE: int = 256
    
    # Database connection
    DATABASE_URL: str
    
    # Data sources configuration
    SOURCES: Dict[str, SourceConfig] = {
        "east_coast": SourceConfig(
            base_url="https://eastcoast.coastwatch.noaa.gov/data",
            satellites={
                "avhrr-viirs": SatelliteConfig(
                    prefix="ACSPOCW",
                    product="MULTISAT",
                    measurement="SST-NGT",
                    resolution="750M",
                    file_format="ACSPOCW_{date}_{time_range}_MULTISAT_SST-NGT_{region}_750M.nc4"
                ),
                "geopolar": SatelliteConfig(
                    prefix="GPBCW",
                    product="GEOPOLAR",
                    measurement="SST",
                    resolution="5KM",
                    file_format="GPBCW_{date}_{time_range}_GEOPOLAR_SST_{region}_5KM.nc4"
                )
            },
            regions=[Region.GULF_MEXICO, Region.EAST_COAST, Region.NORTHEAST, 
                    Region.MID_ATLANTIC, Region.SOUTH_ATLANTIC],
            time_ranges=[TimeRange.DAILY, TimeRange.THREE_DAY, TimeRange.SEVEN_DAY,
                        TimeRange.MONTHLY, TimeRange.SEASONAL, TimeRange.ANNUAL]
        )
    }
    
    class Config:
        env_file = ".env"
        
    def get_file_path(self, source: str, satellite: str, 
                      date: str, time_range: TimeRange, region: Region) -> Path:
        """
        Constructs the file path for a specific data request.
        
        Args:
            source: Data source identifier
            satellite: Satellite identifier
            date: Date string in YYYYDDD format
            time_range: Time range for the data
            region: Geographic region
            
        Returns:
            Path: Complete file path
        """
        source_config = self.SOURCES.get(source)
        if not source_config:
            raise ValueError(f"Unknown source: {source}")
            
        satellite_config = source_config.satellites.get(satellite)
        if not satellite_config:
            raise ValueError(f"Unknown satellite: {satellite} for source: {source}")
            
        filename = satellite_config.file_format.format(
            date=date,
            time_range=time_range.value.upper(),
            region=region.value.upper()
        )
        
        return self.RAW_PATH / source / satellite / filename

settings = Settings()
