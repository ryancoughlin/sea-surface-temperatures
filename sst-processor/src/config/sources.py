from enum import Enum
from typing import Dict, List
from pydantic import BaseModel

class SourceType(str, Enum):
    ERDDAP = "erddap"
    EAST_COAST = "east_coast"

class Dataset(BaseModel):
    name: str
    variable: str
    time_format: str
    is_night_only: bool = True
    resolution: str

class Source(BaseModel):
    type: SourceType
    base_url: str
    datasets: Dict[str, Dataset]

SOURCES = {
    SourceType.ERDDAP: Source(
        type=SourceType.ERDDAP,
        base_url="https://coastwatch.noaa.gov/erddap/griddap",
        datasets={
            "blended_night": Dataset(
                name="noaacwBLENDEDsstDaily",
                variable="analysed_sst",
                time_format="%Y-%m-%dT00:00:00Z",
                resolution="2km"
            ),
            "blended_day_night": Dataset(
                name="noaacwBLENDEDsstDNDaily",
                variable="analysed_sst",
                time_format="%Y-%m-%dT00:00:00Z",
                resolution="2km"
            )
        }
    ),
    SourceType.EAST_COAST: Source(
        type=SourceType.EAST_COAST,
        base_url="https://eastcoast.coastwatch.noaa.gov/data",
        datasets={
            "coastal": Dataset(
                name="ACSPOCW_MULTISAT",
                variable="sst",
                time_format="%Y%m%d",
                resolution="750m"
            )
        }
    )
}
