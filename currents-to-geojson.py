import xarray as xr
import json
import numpy as np

# Load the dataset
file_path = 'data/ne-currents.nc'
ds = xr.open_dataset(file_path, decode_times=False)

# Extract data
latitude = ds['lat'].values
longitude = ds['lon'].values
u_data = ds['u'][0, :, :].values  # Shape is (1440, 719) - (lon, lat)
v_data = ds['v'][0, :, :].values

# Print debug info
print(f"Data shapes - u: {u_data.shape}, v: {v_data.shape}")
print(f"Coordinate shapes - lat: {latitude.shape}, lon: {longitude.shape}")
print(f"Sample values - u: {u_data[0,0]}, v: {v_data[0,0]}")

# Create features using list comprehension
features = [
    {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [float(lon), float(lat)]
        },
        "properties": {
            "u": float(u_data[j, i]),  # j for longitude, i for latitude
            "v": float(v_data[j, i]),
            "velocity": float(np.sqrt(u_data[j, i]**2 + v_data[j, i]**2)),
            "direction": float(np.degrees(np.arctan2(v_data[j, i], u_data[j, i]))),
            "depth": 15
        }
    }
    for i, lat in enumerate(latitude)    # i goes up to 719
    for j, lon in enumerate(longitude)   # j goes up to 1440
    if not (np.isnan(u_data[j, i]) or np.isnan(v_data[j, i]))
    if np.sqrt(u_data[j, i]**2 + v_data[j, i]**2) > 0.01
]

# Create GeoJSON object
geojson_data = {
    "type": "FeatureCollection",
    "metadata": {
        "title": "Ocean Surface Current Analyses Real-time (OSCAR)",
        "depth": "15 meters",
        "timestamp": "2024-10-21",
        "resolution": "0.25 degrees",
        "source": "SSH, WIND, and SST sources"
    },
    "features": features
}

# Write GeoJSON to file
with open("ne_currents_data.geojson", "w") as geojson_file:
    json.dump(geojson_data, geojson_file)

print(f"Created {len(features)} features")
