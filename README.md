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

| **Component**        | **Description**                                                                                                                         |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| **Resolution**       | 0.05° (~5km)                                                                                                                            |
| **Update Frequency** | Daily                                                                                                                                   |
| **Coverage**         | Global                                                                                                                                  |
| **Variables**        | `sea_surface_temperature` (°C)                                                                                                          |
| **Source**           | [NOAA CoastWatch](https://coastwatch.noaa.gov/cwn/products/noaa-geo-polar-blended-global-sea-surface-temperature-analysis-level-4.html) |

#### VIIRS NPP-STAR L3U

High-resolution sea surface temperature data from NOAA's VIIRS satellite.

| **Component**        | **Description**                                                                                                                                                                                                                                                                                                        |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Resolution**       | 0.02° (~2km)                                                                                                                                                                                                                                                                                                           |
| **Update Frequency** | Hourly                                                                                                                                                                                                                                                                                                                 |
| **Coverage**         | Global                                                                                                                                                                                                                                                                                                                 |
| **Variables**        | - `sea_surface_temperature` (K)<br>- `sst_gradient_magnitude` (K/km)<br>- `quality_level` (unitless)<br>- `l2p_flags` (unitless)<br>- `sses_bias` (K)<br>- `sses_standard_deviation` (K)<br>- `dt_analysis` (K)<br>- `wind_speed` (m/s)<br>- `sea_ice_fraction` (unitless)<br>- `aerosol_dynamic_indicator` (unitless) |
| **Source**           | [NOAA/NESDIS/STAR](https://podaac.jpl.nasa.gov/dataset/VIIRS_NPP-STAR-L3U-v2.80)                                                                                                                                                                                                                                       |

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

### Ocean Currents

#### Blended Altimetry-Based Currents

Global current data derived from satellite altimetry.

| **Component**        | **Description**                |
| -------------------- | ------------------------------ |
| **Resolution**       | 0.25° (~25km)                  |
| **Update Frequency** | Daily                          |
| **Coverage**         | Global                         |
| **Variables**        | `u_current`, `v_current` (m/s) |

#### CMEMS Regional Currents

High-resolution current data for the Cape Cod region.

| **Component**        | **Description**   |
| -------------------- | ----------------- |
| **Resolution**       | 0.0833° (~9km)    |
| **Update Frequency** | Daily             |
| **Coverage**         | Cape Cod Region   |
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
