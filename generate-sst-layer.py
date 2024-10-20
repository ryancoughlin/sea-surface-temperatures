import os
import logging
import numpy as np
import xarray as xr
import rasterio
from rasterio.transform import from_origin
import argparse
import warnings
from scipy.interpolate import RegularGridInterpolator
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
import json

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_color_scale(file_path):
    """
    Load color scale from an external JSON file.
    """
    try:
        with open(file_path, 'r') as file:
            colors = json.load(file)["colors"]
            logging.info(f"Successfully loaded color scale from {file_path}")
            return LinearSegmentedColormap.from_list("sst_gradient", colors, N=256)
    except Exception as e:
        logging.error(f"Error loading color scale from {file_path}: {e}")
        raise

def load_data(file_path):
    """
    Load SST data from the NetCDF file.
    """
    try:
        ds = xr.open_dataset(file_path)
        logging.info(f"Successfully loaded data from {file_path}")
        return ds
    except Exception as e:
        logging.error(f"Error loading data from {file_path}: {e}")
        raise

def select_time_slice(data_array, time_index=0):
    """
    Select a specific time slice if the data has a time dimension.
    """
    if 'time' in data_array.dims:
        data_array = data_array.isel(time=time_index)
        logging.info(f"Selected time slice at index {time_index}")
    return data_array

def increase_resolution(sst, lat_data, lon_data, interpolation_factor):
    """
    Increase data resolution using RegularGridInterpolator for efficiency.
    """
    logging.info("Starting resolution increase...")
    # Extract data
    lats = lat_data.values
    lons = lon_data.values
    sst_values = sst.values

    # Define the interpolator on the original grid
    interpolator = RegularGridInterpolator((lats[:, 0], lons[0, :]), sst_values, method='linear', bounds_error=False, fill_value=np.nan)

    # Create high-resolution grid based on the interpolation factor
    new_lat = np.linspace(lats.min(), lats.max(), int(lats.shape[0] * interpolation_factor))
    new_lon = np.linspace(lons.min(), lons.max(), int(lons.shape[1] * interpolation_factor))
    new_lon_grid, new_lat_grid = np.meshgrid(new_lon, new_lat)
    logging.info(f"High-resolution grid created with shape {new_lon_grid.shape}")

    # Interpolate SST on the new grid
    logging.info("Starting interpolation...")
    new_points = np.array([new_lat_grid.ravel(), new_lon_grid.ravel()]).T
    sst_highres = interpolator(new_points).reshape(new_lat_grid.shape)
    logging.info("Interpolation complete.")

    return xr.DataArray(sst_highres, coords=[new_lat, new_lon], dims=['latitude', 'longitude'])

def celsius_to_fahrenheit(data_array):
    """
    Convert Celsius to Fahrenheit.
    """
    return (data_array * 9 / 5) + 32

def save_as_png_with_contours(data_array, output_file_path, colormap):
    """
    Save the SST data as a high-resolution PNG file for visualization with contour lines.
    Contour lines are added to highlight areas where cold and warm waters meet.
    """
    logging.info(f"Saving data with contours to PNG at {output_file_path}...")
    plt.figure(figsize=(10, 8), dpi=300)

    # Plot filled color map
    plt.pcolormesh(data_array.longitude, data_array.latitude, data_array, cmap=colormap)

    # Add contour lines to indicate temperature breaks
    contour_levels = np.linspace(data_array.min(), data_array.max(), 10)  # Define the temperature breakpoints
    plt.contour(data_array.longitude, data_array.latitude, data_array, levels=contour_levels, colors='black', linewidths=0.5)

    plt.axis('off')
    plt.savefig(output_file_path, bbox_inches='tight', pad_inches=0)
    plt.close()
    logging.info(f"Saved PNG with contours to {output_file_path}")

def main(input_file_path, color_scale_path, interpolation_factor):
    """
    Main workflow to generate high-resolution SST tiles for multiple zoom levels.
    """
    try:
        # Load color scale
        sst_colormap = load_color_scale(color_scale_path)

        # Load data
        ds = load_data(input_file_path)
        sst = ds['sst'].isel(level=0, time=0)  # Extract the first level and time slice
        lat_data = ds['lat']
        lon_data = ds['lon']

        # Adjust interpolation factors based on zoom levels
        zoom_levels = {5: 1.0, 8: 3.0, 12: 6.0}  # Significantly higher interpolation for higher zoom levels
        for zoom, factor in zoom_levels.items():
            logging.info(f"Processing zoom level {zoom} with interpolation factor {factor}...")
            # Increase resolution for the given zoom level
            sst_highres = increase_resolution(sst, lat_data, lon_data, factor)

            # Convert to Fahrenheit
            logging.info("Converting SST data to Fahrenheit...")
            sst_fahrenheit = celsius_to_fahrenheit(sst_highres)

            # Save as PNG with contours for visualization purposes
            output_file_path = f"output_highres_sst_zoom_{zoom}.png"
            save_as_png_with_contours(sst_fahrenheit, output_file_path, sst_colormap)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate high-resolution SST images for multiple zoom levels")
    parser.add_argument("input_file", help="Path to the NetCDF file")
    parser.add_argument("color_scale_file", help="Path to the JSON file containing the color scale")
    parser.add_argument("--interpolation", type=float, default=1.2, help="Interpolation factor (lower to keep more detail)")

    args = parser.parse_args()

    main(args.input_file, args.color_scale_file, interpolation_factor=args.interpolation)
