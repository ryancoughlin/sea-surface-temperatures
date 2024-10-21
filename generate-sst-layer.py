import os
import logging
import numpy as np
import xarray as xr
import json
import argparse
import warnings
from osgeo import gdal
from scipy.ndimage import gaussian_filter
import rasterio
from rasterio.transform import from_origin
from rasterio.enums import Resampling
import cv2

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
            return colors  # Just return color list to use with GDAL
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

def celsius_to_fahrenheit(data_array):
    """
    Convert Celsius to Fahrenheit.
    """
    return (data_array * 9 / 5) + 32

def generate_high_res_image_with_xarray(input_file_path, output_path_tiff, output_path_png):
    """
    Generate a high-resolution image using Xarray and Rasterio for smoother rendering.
    """
    try:
        # Load the data using Xarray
        ds = load_data(input_file_path)
        if 'sst' not in ds:
            raise ValueError("Variable 'sst' not found in the dataset.")
        data = ds['sst'].isel(time=0).values  # Extract the first time slice

        # Remove extra dimensions to ensure data is 2D
        data = np.squeeze(data)

        # Verify data is not None
        if data is None:
            raise ValueError("Failed to read data from the dataset. Data is None.")

        # Convert to Fahrenheit
        logging.info("Converting SST data to Fahrenheit...")
        data_fahrenheit = celsius_to_fahrenheit(data)

        # Apply Gaussian smoothing to improve visual quality
        data_smoothed = gaussian_filter(data_fahrenheit, sigma=1)

        # Normalize data to 8-bit for better compatibility
        data_normalized = ((data_smoothed - np.min(data_smoothed)) / (np.max(data_smoothed) - np.min(data_smoothed)) * 255).astype(np.uint8)

        # Extract geospatial information
        lon = ds['x'].values
        lat = ds['y'].values
        transform = from_origin(lon.min(), lat.max(), lon[1] - lon[0], lat[1] - lat[0])

        # Write data to GeoTIFF using Rasterio
        with rasterio.open(
            output_path_tiff,
            'w',
            driver='GTiff',
            height=data_normalized.shape[0],
            width=data_normalized.shape[1],
            count=1,
            dtype=data_normalized.dtype,
            crs='+proj=latlong',
            transform=transform,
            compress='lzw'  # Add compression for better compatibility with preview applications
        ) as dst:
            dst.write(data_normalized, 1)

        logging.info(f"High-resolution GeoTIFF image generated at {output_path_tiff}")

        # Save as PNG for better compatibility with macOS Preview
        logging.info(f"Saving image as PNG for better compatibility...")
        cv2.imwrite(output_path_png, data_normalized)
        logging.info(f"High-resolution PNG image generated at {output_path_png}")

    except Exception as e:
        logging.error(f"Error during image generation: {e}")
        raise

def main(input_file_path, color_scale_path):
    """
    Main workflow to generate a high-resolution SST image using Xarray and Rasterio.
    """
    try:
        # Load color scale (currently not applied in image generation, but kept for reference)
        sst_colormap = load_color_scale(color_scale_path)

        # Generate high-resolution image with Xarray
        output_path_tiff = "high_res_sst_image_xarray.tif"
        output_path_png = "high_res_sst_image_xarray.png"
        logging.info(f"Generating high-resolution image for maximum detail...")
        generate_high_res_image_with_xarray(input_file_path, output_path_tiff, output_path_png)

        logging.info("High-resolution image generation complete.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a high-resolution SST image using Xarray and Rasterio")
    parser.add_argument("input_file", help="Path to the NetCDF file")
    parser.add_argument("color_scale", help="Path to the JSON file containing the color scale")

    args = parser.parse_args()

    main(args.input_file, args.color_scale)
