# Below is the architecture of the app

## Who uses this data?

- Offshore fisherman across the world.
- These are not scientists and except data to be in a format that is easy to understand and use. E.g "Eddy here" vs seeing raw data.

## What needs to happen?

- Fetch data from either CMEMS or ERDDAP


## Configuration and Responsibilites

- DataProcessor takes care of preprocessing, process once and use
- DataAssembler responsible for creating the JSON for the front-end API endpoint. Generated to output/metadata.json
- Dataset config lives in config/settings.py.
- Region config lives in config/regions.py.

## Data Fetching

- Fetched from CMEMS and ERDDAP from NOAA
- Data returned is per-region, no masking required

## Processing

##
