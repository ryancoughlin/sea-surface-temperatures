import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.path import Path
import geopandas as gpd
from shapely.geometry import Polygon

def create_isotherms_and_pockets(file_path, temp_tolerance):
	ds = xr.open_dataset(file_path)
	sst = ds['sea_surface_temperature'].isel(time=0).squeeze()
	sst = (sst * 9/5) + 32  # Fahrenheit conversion

	fig, ax = plt.subplots()
	contour = ax.contour(sst.lon, sst.lat, sst, levels=np.arange(sst.min(), sst.max(), temp_tolerance), colors='blue')
	filled_contour = ax.contourf(sst.lon, sst.lat, sst, levels=np.arange(sst.min(), sst.max(), temp_tolerance))

	polygons = []
	temp_values = []

	for seg in filled_contour.allsegs:
		for region in seg:
			if len(region) > 3 and np.array_equal(region[0], region[-1]):
				poly = Polygon(region)
				if poly.is_valid:
					path = Path(region)
					mask = path.contains_points(np.column_stack((sst.lon.values.flatten(), sst.lat.values.flatten())))
					masked_sst = sst.values.flatten()[mask]
					if masked_sst.size > 0:
						mean_temp = np.mean(masked_sst)
						if np.isfinite(mean_temp):  # Check if the computed mean is finite
							polygons.append(poly)
							temp_values.append(mean_temp)

	gdf = gpd.GeoDataFrame({'geometry': polygons, 'mean_temp': temp_values}, crs='EPSG:4326')
	gdf.plot(column='mean_temp', cmap='viridis', legend=True, edgecolor='black')
	plt.show()

	return gdf

# Example parameters
file_path = 'fl.nc4'
temp_tolerance = 2  # Adjust this based on your data range and desired resolution
gdf = create_isotherms_and_pockets(file_path, temp_tolerance)

# Optionally, save to GeoJSON
gdf.to_file("isotherms.geojson", driver='GeoJSON')
