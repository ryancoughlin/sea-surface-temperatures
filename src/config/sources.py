from typing import Dict, List
from .settings import Region, TimeRange, SatelliteConfig, SourceConfig

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
