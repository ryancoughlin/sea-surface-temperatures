import xarray as xr
import numpy as np
from PIL import Image
import matplotlib.pyplot as plt
from scipy.ndimage import gaussian_filter
import json
from matplotlib.colors import LinearSegmentedColormap

def load_nc4_file(filepath):
    ds = xr.open_dataset(filepath)
    sst = ds['sst'].isel(time=0, level=0)
    return sst.values, sst.x.values, sst.y.values

def celsius_to_fahrenheit(sst):
    return np.where(np.isnan(sst), np.nan, (sst * 9/5) + 32)

def load_color_scale(filepath):
    with open(filepath, 'r') as f:
        return json.load(f)['colors']

def create_custom_colormap(colors):
    return LinearSegmentedColormap.from_list("custom", colors, N=256)

def create_ultra_smooth_sst_map(sst_data, vmin, vmax, cmap, sigma=1.0, upscale_factor=2):
    # Upscale the SST data
    height, width = sst_data.shape
    sst_upscaled = np.repeat(np.repeat(sst_data, upscale_factor, axis=0), upscale_factor, axis=1)
    
    # Apply Gaussian smoothing to the upscaled SST data
    sst_smooth = gaussian_filter(sst_upscaled, sigma=sigma * upscale_factor)
    
    # Normalize the smoothed SST data
    sst_norm = (sst_smooth - vmin) / (vmax - vmin)
    sst_norm = np.clip(sst_norm, 0, 1)
    
    # Apply the colormap directly to the normalized, smoothed SST data
    colored_sst = cmap(sst_norm)
    
    # Convert to 8-bit RGB
    colored_sst_8bit = (colored_sst[:, :, :3] * 255).astype(np.uint8)
    
    return Image.fromarray(colored_sst_8bit)

def process_ultra_smooth_sst_map(nc4_filepath, color_scale_filepath, output_filepath, sigma=1.0, upscale_factor=2):
    sst, x, y = load_nc4_file(nc4_filepath)
    sst_fahrenheit = celsius_to_fahrenheit(sst)
    
    vmin, vmax = np.nanmin(sst_fahrenheit), np.nanmax(sst_fahrenheit)
    print(f"Original temperature range: {vmin:.2f}°F to {vmax:.2f}°F")
    
    # Adjust vmin and vmax to focus on the most relevant temperature range
    vmin = max(vmin, 50)  # Adjust lower bound if needed
    vmax = min(vmax, 85)  # Adjust upper bound if needed
    print(f"Adjusted temperature range: {vmin:.2f}°F to {vmax:.2f}°F")
    
    colors = load_color_scale(color_scale_filepath)
    custom_cmap = create_custom_colormap(colors)
    
    smooth_sst_image = create_ultra_smooth_sst_map(sst_fahrenheit, vmin, vmax, custom_cmap, sigma=sigma, upscale_factor=upscale_factor)
    smooth_sst_image.save(output_filepath)
    print(f"Ultra-smooth SST map saved as {output_filepath}")

# Usage
nc4_filepath = './data/capecod.nc4'
color_scale_filepath = 'color_scale.json'
output_filepath = './public/capecod_sst_ultra_smooth.png'
process_ultra_smooth_sst_map(nc4_filepath, color_scale_filepath, output_filepath, sigma=1.0, upscale_factor=4)