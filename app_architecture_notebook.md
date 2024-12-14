# Below is the architecture of the app

- Script runs from main.py which calls the ProcessingManager class
- MetadataAssembler responsible for creating the metadata JSON for the front-end API endpoint (includes paths to data and image, ranges, etc.)
- Dataset config lives in config/settings.py. Source of truth for the variables, units, etc.
