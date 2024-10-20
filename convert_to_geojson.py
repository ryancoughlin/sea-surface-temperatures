import xarray as xr
import json
import numpy as np
import os

def load_dataset(file_path):
	"""Load the NetCDF file and return the dataset."""
	return xr.open_dataset(file_path)

def extract_data(dataset):
	"""Extract latitude, longitude, and sea surface temperature data from the dataset."""
	latitudes = dataset['latitude'].values
	longitudes = dataset['longitude'].values
	sst = dataset['sea_surface_temperature'].values[0]  # Assuming time index 0
	return latitudes, longitudes, sst

def celsius_to_fahrenheit(celsius):
	"""Convert Celsius to Fahrenheit."""
	return celsius * 9 / 5 + 32

def generate_features(latitudes, longitudes, sst):
	"""Generate GeoJSON features from the extracted data."""
	features = []
	for i in range(len(latitudes)):
		for j in range(len(longitudes)):
			if not np.isnan(sst[i, j]):  # Skip NaN values
				feature = {
					"type": "Feature",
					"geometry": {
						"type": "Point",
						"coordinates": [
							round(float(longitudes[j]), 3),
							round(float(latitudes[i]), 3)
						]
					},
					"properties": {
						"sea_surface_temperature": round(celsius_to_fahrenheit(float(sst[i, j])), 1)
					}
				}
				features.append(feature)
	return features

def save_features_to_geojson(features, output_path):
	"""Save GeoJSON features as line-delimited GeoJSON."""
	with open(output_path, 'w') as f:
		for feature in features:
			f.write(json.dumps(feature) + '\n')

def main(file_path):
	"""Main function to handle the conversion process."""
	dataset = load_dataset(file_path)
	latitudes, longitudes, sst = extract_data(dataset)
	features = generate_features(latitudes, longitudes, sst)
	
	output_dir = os.path.dirname(file_path)
	output_filename = os.path.splitext(os.path.basename(file_path))[0] + '.geojson'
	output_path = os.path.join(output_dir, output_filename)
	
	save_features_to_geojson(features, output_path)
	print(f"Line-delimited GeoJSON file saved to {output_path}")

if __name__ == "__main__":
	file_path = './data/sst/ne_sample_sst.nc'  # Update this with your file path
	main(file_path)
