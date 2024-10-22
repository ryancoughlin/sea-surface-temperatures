import xarray as xr
import numpy as np
from scipy.interpolate import griddata
import plotly.graph_objects as go

# Load the data
ds = xr.open_dataset('./data/capecod.nc4')
lat = ds['lat']
lon = ds['lon']
sst = ds['sst'].squeeze()
sst_masked = np.ma.masked_invalid(sst)

# Prepare original grid points
points = np.array([lon.values.flatten(), lat.values.flatten()]).T
values = sst_masked.flatten()

# Create high-resolution grid
grid_res = 2000
grid_lon = np.linspace(lon.min(), lon.max(), grid_res)
grid_lat = np.linspace(lat.min(), lat.max(), grid_res)
grid_lon, grid_lat = np.meshgrid(grid_lon, grid_lat)

# Interpolate data
sst_interpolated = griddata(points, values, (grid_lon, grid_lat), method='cubic')

# Handle NaNs
sst_interpolated = np.nan_to_num(sst_interpolated, nan=np.nanmean(sst_interpolated))

# Create Plotly figure
mapbox_access_token = 'pk.eyJ1Ijoic25vd2Nhc3QiLCJhIjoiY2plYXNjdTRoMDhsbDJ4bGFjOWN0YjdzeCJ9.fM2s4NZq_LUiTXJxsl2HbQ'

fig = go.Figure(go.Heatmap(
    x=grid_lon[0],
    y=grid_lat[:, 0],
    z=sst_interpolated,
    colorscale='Viridis',
    colorbar=dict(title='SST (°C)'),
    zsmooth='best'
))

fig.update_layout(
    mapbox=dict(
        accesstoken=mapbox_access_token,
        style='satellite-streets',
        center=dict(lon=float(lon.mean()), lat=float(lat.mean())),
        zoom=6
    ),
    mapbox_style="carto-positron",
    margin={"r": 0, "t": 0, "l": 0, "b": 0}
)

fig.show()
