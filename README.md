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

# AVHRR and VIIRS Multi-Sensor Composite: MUR SST Analysis

| Component          | Description                                                                                        |
|--------------------|----------------------------------------------------------------------------------------------------|
| **Dimensions**     | time: 1, latitude: 451, longitude: 788                                                             |
| **Coordinates**    | - time: 2024-10-23T09:00:00<br>- latitude: 41.51 to 46.01<br>- longitude: -71.16 to -63.29        |
| **Grid Spacing**   | - Latitude Spacing: Approx. 0.01 degrees (~0.7 miles)<br>- Longitude Spacing: Approx. 0.01 degrees (~0.7 miles) |
| **Data Variables** | - analysed_sst (time, latitude, longitude): Sea surface temperature in degrees Celsius             |
| **Data Structure** | This dataset is structured as a 3-dimensional grid with dimensions time, latitude, and longitude. The primary data variable, `analysed_sst`, is stored as a float64 and represents the sea surface temperature at each grid point for the specific timestamp provided. |
| **Attributes**     | - title: Multi-scale Ultra-high Resolution (MUR) SST Analysis<br>- summary: This is a merged, multi-sensor L4 Foundation SST analysis that has been interpolated to a regular latitude/longitude grid.<br>- cdm_data_type: Grid<br>- Conventions: CF-1.6, COARDS, ACDD-1.3<br>- acknowledgement: Please acknowledge the use of these data with the following statement: "These data were provided by JPL MUR SST project."<br>- creator_name: JPL MUR SST project<br>- creator_email: ghrsst@podaac.jpl.nasa.gov<br>- time_coverage_start: 2024-10-23T09:00:00Z<br>- time_coverage_end: 2024-10-23T09:00:00Z<br>- Westernmost_Easting: -71.16 |
