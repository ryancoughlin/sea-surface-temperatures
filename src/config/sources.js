export const DATA_SOURCES = {
  east_coast: {
    base_url: "https://eastcoast.coastwatch.noaa.gov/data",
    satellites: {
      "avhrr-viirs": {
        prefix: "ACSPOCW",
        product: "MULTISAT",
        measurement: "SST-NGT",
        resolution: "750M",
        file_format: (date, timeRange, region) => 
          `ACSPOCW_${date}_${timeRange.toUpperCase()}_MULTISAT_SST-NGT_${region.toUpperCase()}_750M.nc4`
      },
      "geopolar": {
        prefix: "GPBCW",
        product: "GEOPOLAR",
        measurement: "SST",
        resolution: "5KM",
        file_format: (date, timeRange, region) => 
          `GPBCW_${date}_${timeRange.toUpperCase()}_GEOPOLAR_SST_${region.toUpperCase()}_5KM.nc4`
      },
      "mur": {
        // Add MUR specific details here when available
      }
    },
    regions: ["gm", "ec", "ne", "ma", "sa"],
    time_ranges: ["daily", "3day", "7day", "monthly", "seasonal", "annual"]
  },
  // Add other sources here as needed
};
