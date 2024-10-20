# sea-surface-temperatures
API and tile generation for SST data from NOAA


## Data Sources

### AVHRR and VIIRS Multi-Sensor Composite
Link: https://eastcoast.coastwatch.noaa.gov/cw_avhrr-viirs_sst.php

| **Component**      | **Description**                                                                  |
|--------------------|----------------------------------------------------------------------------------|
| **Dimensions**     | `x: 680`, `y: 820`, `time: 1`, `n_vals: 2`, `level: 1`                           |
| **Grid Density**   | Resolution of roughly `680 x 820` points across the area of interest             |
| **Grid Spacing**   | - **Latitude Spacing**: ~0.27 miles                                             |
|                    | - **Longitude Spacing**: ~0.27 miles                                            |
| **Coordinates**    | - `x (x)`: Float values (680 elements)                                           |
|                    | - `y (y)`: Float values (820 elements)                                           |
|                    | - `lat (y, x)`: Latitude array (820 x 680 elements)                              |
|                    | - `lon (y, x)`: Longitude array (820 x 680 elements)                             |
|                    | - `time (time)`: Timestamp - `2024-10-16T04:19:59`                                |
|                    | - `level (level)`: Surface level indicator (`0.0`)                                |
| **Data Variables** | - `coord_ref`: Integer value                                                     |
|                    | - `time_bounds (time, n_vals)`: Time boundaries (2 elements per time)            |
|                    | - `sst (time, level, y, x)`: Sea surface temperature (4D array)                  |
| **Attributes**     | - Conventions (`CF-1.4`)                                                         |
|                    | - Source (`METOPB_METOPC_NOAA20_NOAA21_SNPP_AVHRR_VIIRS`)                        |
|                    | - Institution (`NOAA/NESDIS/OSPO`)                                               |
|                    | - History, createInstitution, createDateTime, etc. (22 attributes total)         |
