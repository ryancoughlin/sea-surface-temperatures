prompt.md

Prompt:

We are developing a Python-based tool to process and visualize sea surface temperature (SST) data for offshore fishing maps. The SST data is sourced from NOAA and is provided in NetCDF (.nc4) format. This dataset contains sea surface temperature measurements across a regular grid, with the following dimensions:

Grid Size: 680 (longitude) x 820 (latitude) points
Grid Spacing: ~0.27 miles per point in both latitude and longitude
Time Dimension: One timestamp (2024-10-16T04:19:59)
Temperature Units: Celsius (to be converted to Fahrenheit for display)
Variables: SST (4D array: time, level, y, x), latitude, longitude
Project Requirements:
Objective: Display high-resolution temperature maps to assist with offshore fishing by identifying key temperature breaks (0.5 to 1 degree Fahrenheit differences). This helps locate oceanic fronts and areas of interest for fishing.

Zoom Levels:

The tool must generate three distinct zoom levels with different visual detail, based on the grid resolution (~0.27 miles per point) and the requirements for fishing maps.
Zoom Level 1 (Wide Region View) - Zoom Level 5:

Scale: Large-scale overview covering a wide offshore area (hundreds of miles).
Detail: Coarser resolution using the original grid (680x820). At this zoom level, smaller temperature breaks (0.5-1°F) may not be visible, but large trends and gradients will stand out.
Output: Export a PNG preview showing this wide region with a coolwarm colormap.
Zoom Level 2 (Intermediate) - Zoom Level 8:

Scale: Mid-scale zoom showing a smaller section of the map (50-100 miles across).
Detail: Interpolate the original grid to generate a finer grid (2x resolution), increasing the grid density from 680x820 to 1360x1640. This will allow visibility of temperature breaks as small as 1°F.
Output: Export a PNG preview showing this region with finer details.
Zoom Level 3 (Fine Detail for Fishing) - Zoom Level 10:

Scale: High zoom focusing on small, localized areas (10-20 miles across).
Detail: Use data density from the file to generate smooth and detailed temperature maps. This level should show very fine temperature gradients, allowing for highly accurate visualization of temperature breaks.
Output: Export a PNG preview showing this zoomed-in view with the highest level of detail.
Data Transformation:

Convert SST data from Celsius to Fahrenheit.
Apply Gaussian smoothing to the SST data to reduce noise and smooth gradients.
Visualization:

Use a coolwarm colormap (blue for cooler temperatures and red for warmer temperatures) to visualize temperature variations.
Generate and save the visualization as a .png image for each zoom level (5, 8, and 10), providing a preview of the SST map at different scales.
The output should be a map of SST, showing detailed temperature gradients crucial for offshore fishing decisions.
Tools and Libraries:

xarray: For reading and manipulating NetCDF files, handling SST data.
NumPy: For efficient numerical operations and data transformations.
Matplotlib: To apply the coolwarm colormap and normalize the temperature data.
PIL (Pillow): To convert the colored SST data into an image and save it as a .png.
Scipy: For Gaussian filtering to smooth out the data and reduce noise.
Challenges:

Ensure the data is correctly loaded and transformed from the NetCDF format.
Handle singleton dimensions (e.g., one timestamp) correctly and work with grid dimensions (latitude, longitude) to ensure accurate interpolation.
Maintain high resolution for zoomed-in maps, ensuring temperature breaks as small as 0.5°F are visible at the highest zoom level.
Additional Requirements:

No averaging over time, as there's only one timestamp.
Ensure proper data handling, error-free execution, and clarity in temperature visualization.
Use the data density to calculate detail for each zoom level, avoid over-interpolation.
Ability to scale up the process for generating zoomed-in tiles, interpolating data for higher resolutions.
Ignore NaN, null and empty data. Don't interpolate over them. These are points over land.

App Flow and Data Pipeline:

1. Data Processing (Python Script):

   - Runs on a scheduled basis (daily/hourly) via cron job
   - Downloads latest NetCDF files from NOAA
   - Processes SST data for all zoom levels
   - Generates PNG tiles and GeoJSON features
   - Validates data integrity before saving

2. Data Storage:

   - Store in PostgreSQL database with PostGIS extension
   - Schema:
     - sst_data: Stores metadata and file references
     - sst_tiles: Stores generated image tiles
     - sst_features: Stores GeoJSON temperature break features
   - Include timestamp, zoom level, and bounding box metadata

3. API Endpoints:
   - GET /api/sst/{source}/{region}/tiles/{z}/{x}/{y}: Retrieve map tiles
   - GET /api/sst/{source}/{region}/features/{bbox}: Retrieve GeoJSON features
   - GET /api/sst/{source}/{region}/metadata: Get latest data timestamp and coverage
   - GET /api/sources: List available data sources
   - GET /api/regions: List available regions for each source

Caching Strategy:

1. File System Caching:

   - Store generated PNGs and GeoJSON in cloud storage (S3/GCS)
   - Use content-based hashing for cache invalidation

2. API Response Caching:

   - Set Cache-Control headers:
     - max-age=86400 (24 hours) for tiles
     - stale-while-revalidate for real-time updates
   - Implement ETag/If-None-Match for conditional requests

3. Database Caching:
   - Cache frequent queries using Redis
   - Store computed temperature breaks
   - Cache invalidation on new data ingestion

Error Handling:

- Implement retry logic for NOAA data downloads
- Log processing errors with stack traces
- Provide fallback data if latest processing fails

Additional Features:

1. Data Sources Integration:

   - Multiple SST sources (NOAA, NASA, Copernicus) for redundancy
   - Ocean current data integration
   - Chlorophyll concentration maps
   - Sea surface height/altimetry
   - Weather overlay support (wind, waves)

2. Performance Optimizations:

   - Pre-generate common zoom levels during off-peak
   - Vector tiles for temperature break lines
   - Implement WebGL rendering for smooth transitions
   - Use HTTP/2 for parallel tile loading

3. Mobile Considerations:

   - Offline tile downloads for specific regions
   - Bandwidth-efficient tile format (WebP)
   - Progressive loading for slow connections
   - Compressed GeoJSON for features

4. User Features:
   - Waypoint saving with temperature annotations
   - Historical temperature comparison
   - Temperature break notifications
   - Share specific views via URL
   - Export to common marine GPS formats

Advanced Features:

1. Data Integration & Processing:

   - Multi-source data fusion:
     - NOAA GOES-16/17 (primary SST)
     - NASA MODIS (backup SST)
     - Copernicus Sentinel-3 (validation)
     - CMEMS for ocean currents
     - VIIRS for chlorophyll-a
   - Data quality indicators:
     - Cloud coverage percentage
     - Data age/freshness
     - Confidence metrics
   - Automated quality control:
     - Cloud masking
     - Bad data detection
     - Gap filling algorithms

2. API Optimization:

   - Vector Tiles:
     - Temperature break lines as MVT
     - Contour generation at 0.5°F intervals
     - Simplified geometries per zoom level
   - Raster Optimization:
     - WebP with fallback to PNG
     - Variable compression based on zoom
     - Tile size optimization (256x256 vs 512x512)
   - Response Formats:
     - GeoJSON for features
     - Protocol Buffers for dense data
     - JSON-LD for metadata

3. Offline Capabilities:

   - Progressive Web App (PWA):
     - Service worker for offline access
     - Background sync for new data
     - Selective area caching
   - Data Management:
     - Tile package downloads by region
     - Compressed storage format
     - Storage quota management
     - Auto-cleanup of old data

4. Professional Features:

   - Temperature Analysis (future feature):
     - Break detection algorithms
     - Historical temperature trends
     - Temperature gradient calculations
     - Front strength indicators
   - Route Planning (future feature):
     - Temperature-based route optimization
     - Waypoint management with notes
     - Track recording with conditions
     - Export to Garmin/Furuno/Simrad
   - Alerts (future feature:
     - Custom temperature break notifications
     - Significant change detection
     - Area-based monitoring
     - Push notifications when in range

5. Mobile Optimization:
   - Interface:
     - Large touch targets for marine use
     - High contrast for sunlight
     - Night mode for dawn/dusk fishing
