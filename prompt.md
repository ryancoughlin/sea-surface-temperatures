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
