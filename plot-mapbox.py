import xarray as xr
import numpy as np
from scipy.interpolate import griddata
import plotly.graph_objects as go

# Load the data
ds = xr.open_dataset('./data/capecod.nc4')
lat = ds['lat'].values  # Shape: (820, 680)
lon = ds['lon'].values  # Shape: (820, 680)
sst = ds['sst'].squeeze().values  # Shape: (820, 680)

# Flatten the arrays
lat_flat = lat.flatten()
lon_flat = lon.flatten()
sst_flat = sst.flatten()

# Create a mask for valid data (ignore NaNs)
valid_mask = ~np.isnan(sst_flat)

# Extract valid data
lat_valid = lat_flat[valid_mask]
lon_valid = lon_flat[valid_mask]
sst_valid = sst_flat[valid_mask]

# Prepare original grid points
points = np.column_stack((lon_valid, lat_valid))
values = sst_valid

# Create high-resolution grid
grid_res = 2000  # Adjust based on performance needs
grid_lon = np.linspace(lon_valid.min(), lon_valid.max(), grid_res)
grid_lat = np.linspace(lat_valid.min(), lat_valid.max(), grid_res)
grid_lon_mesh, grid_lat_mesh = np.meshgrid(grid_lon, grid_lat)

# Interpolate data
sst_interpolated = griddata(
    points, values, (grid_lon_mesh, grid_lat_mesh), method='linear'
)

# Mask NaNs in interpolated data
sst_interpolated_masked = np.ma.masked_invalid(sst_interpolated)

# Create Plotly figure
mapbox_access_token = 'YOUR_MAPBOX_ACCESS_TOKEN'

fig = go.Figure(go.Heatmap(
    x=grid_lon,
    y=grid_lat,
    z=sst_interpolated_masked,
    colorscale='Viridis',
    colorbar=dict(title='SST (°C)'),
    zsmooth='best'
))

fig.update_layout(
    mapbox=dict(
        accesstoken=mapbox_access_token,
        style='carto-positron',
        center=dict(lon=float(lon_valid.mean()), lat=float(lat_valid.mean())),
        zoom=6
    ),
    mapbox_style="carto-positron",
    margin={"r": 0, "t": 0, "l": 0, "b": 0},
    title="Sea Surface Temperature Map"
)

fig.show()
