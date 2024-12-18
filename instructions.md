Project: Sea Surface Temperature (SST) Data Management System

ERDDAP Requirements:

- ERDDAP is a NOAA service that provides gridded data.
- It requires a base_url, dataset_id, and variables.
- The variables is a list of variables to fetch.
- Date / time format is '%Y-%m-%dT00:00:00Z'

Rules

- Prefer succinct and simple code to start
- When passing around functions and values, prefer to pass around simple values and functions that can be composed to build more complex functionality
- Reduce redundancy from the start
- Build with consistency in mind and modularity. There are many source and types of data to retrieve. I want this to scale and be easy to read. Adding new data types should be easy.
- Break code out to be single responsibility and composable.

  Overview

- App will fetch data from external sources, process it, and expore to image and tiles to display on a map. The map will be a separate app.
- Data sources are from NOAA ERDDAP and NASA PODAAC
- Types of data are SST, currents, waves, chlorophyll, sea ice, bathymetry, etc.
  - SST and currents are the priority
- Regions are defined in config/regions.py
- Settings are defined in config/settings.py
