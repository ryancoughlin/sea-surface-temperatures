import os
import numpy as np
from osgeo import gdal, osr
import matplotlib.pyplot as plt
from scipy.ndimage import gaussian_filter

def load_sst_data(nc4_filepath):
    # Open the NetCDF file using GDAL
    ds = gdal.Open(nc4_filepath)
    if ds is None:
        raise ValueError(f"Could not open {nc4_filepath}")

    # Read the SST band
    band = ds.GetRasterBand(1)
    sst = band.ReadAsArray()

    # Get geotransform information
    geotransform = ds.GetGeoTransform()
    
    # Convert to Fahrenheit
    sst_fahrenheit = (sst - 273.15) * 9/5 + 32
    
    return sst_fahrenheit, geotransform

def apply_smoothing(sst, sigma=1):
    return gaussian_filter(sst, sigma=sigma)

def create_colormap():
    return plt.get_cmap('coolwarm')

def save_sst_image(sst, geotransform, output_path, colormap, zoom_level):
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Normalize SST data
    vmin, vmax = np.nanmin(sst), np.nanmax(sst)
    normalized_sst = (sst - vmin) / (vmax - vmin)

    # Apply colormap
    colored_sst = (colormap(normalized_sst) * 255).astype(np.uint8)

    # Create a new raster for the colored SST
    driver = gdal.GetDriverByName('GTiff')
    out_ds = driver.Create(output_path, sst.shape[1], sst.shape[0], 3, gdal.GDT_Byte)
    
    # Set the geotransform and projection
    out_ds.SetGeoTransform(geotransform)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)  # WGS84
    out_ds.SetProjection(srs.ExportToWkt())

    # Write the colored SST data
    for i in range(3):
        out_ds.GetRasterBand(i+1).WriteArray(colored_sst[:,:,i])

    out_ds = None  # Close the dataset

    print(f"Saved SST image for zoom level {zoom_level} to {output_path}")

def main():
    nc4_filepath = './data/capecod.nc4'
    output_dir = './output'
    
    # Load and process SST data
    sst, geotransform = load_sst_data(nc4_filepath)
    
    # Apply smoothing
    smoothed_sst = apply_smoothing(sst)
    
    # Create colormap
    colormap = create_colormap()
    
    # Generate images for different zoom levels
    zoom_levels = [5, 8, 10]
    for zoom in zoom_levels:
        output_path = os.path.join(output_dir, f'sst_map_zoom_{zoom}.tif')
        save_sst_image(smoothed_sst, geotransform, output_path, colormap, zoom)

if __name__ == "__main__":
    main()
