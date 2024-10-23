from typing import Dict, Tuple
from enum import Enum
from pydantic import BaseModel

class RegionCode(str, Enum):
    GULF_MEXICO = "gm"
    EAST_COAST = "ec"
    NORTHEAST = "ne"
    MID_ATLANTIC = "ma"
    SOUTH_ATLANTIC = "sa"

class RegionBounds(BaseModel):
    name: str
    description: str
    lon: Tuple[float, float]
    lat: Tuple[float, float]

class Region(BaseModel):
    code: RegionCode
    name: str
    description: str
    bounds: RegionBounds

REGIONS: Dict[RegionCode, Region] = {
    RegionCode.GULF_MEXICO: Region(
        code=RegionCode.GULF_MEXICO,
        name="Gulf of Mexico",
        description="Gulf of Mexico coastal waters",
        bounds=RegionBounds(
            name="Gulf of Mexico",
            description="Gulf Coast region from Texas to Florida",
            lon=(-98.0, -80.0),
            lat=(18.0, 31.0)
        )
    ),
    RegionCode.EAST_COAST: Region(
        code=RegionCode.EAST_COAST,
        name="East Coast",
        description="Full US East Coast waters",
        bounds=RegionBounds(
            name="East Coast",
            description="Atlantic Coast from Florida to Maine",
            lon=(-82.0, -65.0),
            lat=(25.0, 45.0)
        )
    ),
    RegionCode.NORTHEAST: Region(
        code=RegionCode.NORTHEAST,
        name="Northeast",
        description="Northeast US coastal waters",
        bounds=RegionBounds(
            name="Northeast",
            description="New England coastal waters",
            lon=(-76.0, -65.0),
            lat=(35.0, 45.0)
        )
    ),
    RegionCode.MID_ATLANTIC: Region(
        code=RegionCode.MID_ATLANTIC,
        name="Mid-Atlantic",
        description="Mid-Atlantic coastal waters",
        bounds=RegionBounds(
            name="Mid-Atlantic",
            description="NY to VA coastal waters",
            lon=(-77.0, -70.0),
            lat=(35.0, 41.0)
        )
    ),
    RegionCode.SOUTH_ATLANTIC: Region(
        code=RegionCode.SOUTH_ATLANTIC,
        name="South Atlantic",
        description="South Atlantic coastal waters",
        bounds=RegionBounds(
            name="South Atlantic",
            description="FL to NC coastal waters",
            lon=(-82.0, -76.0),
            lat=(25.0, 35.0)
        )
    )
}
