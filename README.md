# Sea Surface Data API

API and tile generation service for oceanographic data focusing on the Cape Cod region. This service processes and serves data from multiple sources including sea surface temperatures, currents, and chlorophyll concentrations.

## Features

- Real-time data processing from NOAA, CMEMS, and other oceanographic sources
- Standardized API endpoints for accessing processed data
- Map tile generation for visualization
- Data interpolation and gap-filling where needed

## Data Sources

### Sea Surface Temperature (SST)

#### NOAA Geo-Polar Blended SST Analysis

High-resolution global sea surface temperature analysis combining multiple satellite sources.

| **Component**      | **Description**                                                                                                                         |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| **Resolution**     | 0.05° (~5km)                                                                                                                            |
| **Frequency**      | Daily                                                                                                                                   |
| **Coverage**       | Global                                                                                                                                  |
| **Dimensions**     | `time`, `latitude`, `longitude`                                                                                                         |
| **Grid Density**   | High resolution ~0.05°                                                                                                                  |
| **Grid Spacing**   | - **Latitude Spacing**: ~0.05°                                                                                                          |
|                    | - **Longitude Spacing**: ~0.05°                                                                                                         |
| **Data Variables** | - `sea_surface_temperature`: float32 (units: °C)                                                                                        |
|                    | - `quality_level`: Quality control level of SST measurements                                                                            |
|                    | - `sst_anomaly`: Sea surface temperature anomaly relative to climatology                                                                |
| **Source**         | [NOAA CoastWatch](https://coastwatch.noaa.gov/cwn/products/noaa-geo-polar-blended-global-sea-surface-temperature-analysis-level-4.html) |
| **Array Storage**  | Arrays are stored in C-style (row-major order), optimized for efficient processing and visualization.                                   |
| **Attributes**     | - `Conventions`: CF-1.6, ACDD-1.3                                                                                                       |
|                    | - `summary`: High-resolution global SST combining geostationary and polar-orbiting satellite measurements                               |
|                    | - `title`: NOAA Geo-Polar Blended Global SST Analysis Level-4                                                                           |

#### AVHRR and VIIRS Multi-Sensor Composite

High-resolution sea surface temperature data from NOAA satellites.

| **Component**        | **Description**                                                                 |
| -------------------- | ------------------------------------------------------------------------------- |
| **Resolution**       | 0.02° (~2km)                                                                    |
| **Update Frequency** | Daily                                                                           |
| **Coverage**         | Global                                                                          |
| **Variables**        | `sea_surface_temperature` (°C)                                                  |
| **Source**           | [NOAA CoastWatch](https://eastcoast.coastwatch.noaa.gov/cw_avhrr-viirs_sst.php) |

#### CMEMS Sea Water Temperature

Regional temperature data with higher accuracy for the Cape Cod area.

| **Component**        | **Description**   |
| -------------------- | ----------------- |
| **Resolution**       | 0.0833° (~9km)    |
| **Update Frequency** | Daily             |
| **Coverage**         | Cape Cod Region   |
| **Variables**        | `thetao` (°C)     |
| **Source**           | CMEMS MOD GLO PHY |

### SST

#### VIIRS NPP-STAR-L3U-v2.80

| **Component**      | **Description**                                                                                      |
| ------------------ | ---------------------------------------------------------------------------------------------------- |
| **Frequency**      | Every 10 minutes (144 granules per day)                                                              |
| **Dimensions**     | `time`, `latitude`, `longitude`                                                                      |
| **Grid Density**   | High resolution ~0.25°                                                                               |
| **Grid Spacing**   | - **Latitude Spacing**: ~0.0200°                                                                     |
|                    | - **Longitude Spacing**: ~0.0200°                                                                    |
| **Coordinates**    | - `time`                                                                                             |
|                    | - `latitude`                                                                                         |
|                    | - `longitude`                                                                                        |
| **Data Variables** | - `sea_surface_temperature`: float32                                                                 |
|                    | - `quality_level`: Quality control indicator for SST values                                          |
|                    | - `sses_bias`: Sensor-specific error statistics for bias correction                                  |
|                    | - `sses_standard_deviation`: Sensor-specific error statistics for standard deviation                 |
|                    | - `wind_speed`: float32, wind speed associated with SST measurements                                 |
|                    | - `aerosol_dynamic_indicator`: float32, aerosol content estimate                                     |
|                    | - `l2p_flags`: Granule-level flags indicating processing status                                      |
| **Array Storage**  | Arrays are stored in C-style (row-major order), optimized for efficient processing and visualization |
| **Attributes**     | - `Conventions`: CF-1.7, ACDD-1.3                                                                    |
|                    | - `summary`: High-frequency SST retrievals produced by NOAA/NESDIS/STAR office                       |
|                    | - `title`: NOAA STAR VIIRS Level-3 Uncollated SST V2.80                                              |

### Ocean Currents

#### CMEMS Regional Currents

High-resolution current data for the Cape Cod region.

| **Component**        | **Description**   |
| -------------------- | ----------------- |
| **Resolution**       | 0.0833° (~9km)    |
| **Update Frequency** | Daily             |
| **Coverage**         | Global            |
| **Variables**        | `uo`, `vo` (m/s)  |
| **Source**           | CMEMS MOD GLO PHY |

### Water Quality

#### Chlorophyll OCI VIIRS

Gap-filled chlorophyll concentration data.

| **Component**        | **Description**          |
| -------------------- | ------------------------ |
| **Resolution**       | 0.0833° (~9km)           |
| **Update Frequency** | Daily                    |
| **Coverage**         | Global                   |
| **Variables**        | `chlor_a` (mg/m³)        |
| **Source**           | NOAA S-NPP NOAA-20 VIIRS |

## Data Processing

All data arrays are stored in row-major order (C-style) layout and follow CF conventions for metadata. Processing includes:

- Standardization of coordinate systems
- Quality control and validation
- Gap-filling where applicable
- Optimization for visualization tasks

## Attribution

Data provided by:

- NOAA/NESDIS/STAR
- E.U. Copernicus Marine Service Information (CMEMS)
- Mercator Ocean International
