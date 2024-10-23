import sys
import os
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import RegularGridInterpolator
import json
from scipy.signal import savgol_filter
import traceback

def load_sst_data(nc4_filepath):
    with xr.open_dataset(nc4_filepath) as ds:
        sst = ds.sst.squeeze().values  # Remove singleton dimensions
        lat = ds.lat.values
        lon = ds.lon.values
    
    # Convert to Fahrenheit
    sst_fahrenheit = (sst * 9/5) + 32
    
    print("Fahrenheit SST data stats:")
    print(f"Shape: {sst_fahrenheit.shape}")
    print(f"Min: {np.nanmin(sst_fahrenheit):.2f}, Max: {np.nanmax(sst_fahrenheit):.2f}")
    print(f"NaN count: {np.isnan(sst_fahrenheit).sum()}")
    
    return sst_fahrenheit, lat, lon

def bilinear_interpolate(sst, scale_factor):
    rows, cols = sst.shape
    new_rows, new_cols = rows * scale_factor, cols * scale_factor
    
    # Create coordinate arrays for original and new grids
    x = np.arange(cols)
    y = np.arange(rows)
    new_x = np.linspace(0, cols - 1, new_cols)
    new_y = np.linspace(0, rows - 1, new_rows)
    
    # Calculate interpolation weights
    x_floor = np.floor(new_x).astype(int)
    y_floor = np.floor(new_y).astype(int)
    x_weight = new_x - x_floor
    y_weight = new_y - y_floor
    
    # Ensure we don't go out of bounds
    x_ceil = np.minimum(x_floor + 1, cols - 1)
    y_ceil = np.minimum(y_floor + 1, rows - 1)
    
    # Perform bilinear interpolation
    top_left = sst[y_floor[:, None], x_floor]
    top_right = sst[y_floor[:, None], x_ceil]
    bottom_left = sst[y_ceil[:, None], x_floor]
    bottom_right = sst[y_ceil[:, None], x_ceil]
    
    interpolated = (top_left * (1 - x_weight) * (1 - y_weight[:, None]) +
                    top_right * x_weight * (1 - y_weight[:, None]) +
                    bottom_left * (1 - x_weight) * y_weight[:, None] +
                    bottom_right * x_weight * y_weight[:, None])
    
    return np.round(interpolated, 2)

def interpolate_sst(sst, scale_factor):
    # Create a mask for valid data points
    mask = ~np.isnan(sst)
    y, x = np.indices(sst.shape)
    
    # Extract valid data points
    valid_points = np.array((y[mask], x[mask])).T
    valid_values = sst[mask]
    
    # Define the original grid
    original_grid = (np.arange(sst.shape[0]), np.arange(sst.shape[1]))
    
    # Create the interpolator
    interpolator = RegularGridInterpolator(original_grid, sst, bounds_error=False, fill_value=np.nan)
    
    # Define the new grid
    new_y = np.linspace(0, sst.shape[0] - 1, sst.shape[0] * scale_factor)
    new_x = np.linspace(0, sst.shape[1] - 1, sst.shape[1] * scale_factor)
    new_grid = np.meshgrid(new_y, new_x, indexing='ij')
    
    # Interpolate the data
    interpolated_sst = interpolator((new_grid[0], new_grid[1]))
    
    # Round to four decimal points for more detail
    interpolated_sst = np.round(interpolated_sst, 8)
    
    return interpolated_sst

def smooth_sst_with_savgol(sst, window_length=11, polyorder=2):
    # Apply Savitzky-Golay filter along each axis
    smoothed_sst = savgol_filter(sst, window_length=window_length, polyorder=polyorder, axis=0, mode='nearest')
    smoothed_sst = savgol_filter(smoothed_sst, window_length=window_length, polyorder=polyorder, axis=1, mode='nearest')
    return smoothed_sst

def increase_resolution(sst, lat, lon, scale_factor):
    # Smooth the SST data before interpolation
    smoothed_sst = smooth_sst_with_savgol(sst, window_length=11, polyorder=2)
    
    high_res_sst = interpolate_sst(smoothed_sst, scale_factor)
    
    # Sample from the top right quadrant
    rows, cols = sst.shape
    sample_y, sample_x = rows // 4, cols * 3 // 4  # Top right quadrant
    
    # Find the first non-NaN 2x2 sample in this quadrant
    for y in range(sample_y, min(sample_y + 50, rows - 1)):
        for x in range(sample_x, min(sample_x + 50, cols - 1)):
            if not np.isnan(sst[y:y+2, x:x+2]).any():
                print("Original sample:")
                print(np.round(sst[y:y+2, x:x+2], 2))
                print("\nInterpolated sample:")
                print(high_res_sst[y*scale_factor:(y+2)*scale_factor, x*scale_factor:(x+2)*scale_factor])
                return high_res_sst
    
    print("No valid 2x2 non-NaN sample found in the top right quadrant.")
    return high_res_sst

def save_sst_image(sst, output_path, zoom_level, vmin, vmax):
    fig, ax = plt.subplots(figsize=(10, 12))
    
    with open('color_scale.json', 'r') as f:
        color_scale = json.load(f)
    colors = color_scale['colors']
    cmap = plt.cm.colors.LinearSegmentedColormap.from_list('custom_cmap', colors)
    
    # Set NaN values to be fully transparent in the colormap
    cmap.set_bad(alpha=0)
    
    # Plot the SST data using imshow
    ax.imshow(sst, cmap=cmap, vmin=vmin, vmax=vmax, 
              extent=[0, sst.shape[1], 0, sst.shape[0]])
    
    # Remove all axes, labels, and borders
    ax.axis('off')
    plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
    plt.margins(0,0)
    
    # Save the image without any padding and with transparency
    plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0, transparent=True)
    plt.close(fig)
    print(f"Saved SST image for zoom level {zoom_level} to {output_path}")

def process_zoom_levels(sst, lat, lon, output_dir):
    # Calculate global min and max for consistent color scale
    valid_data = sst[~np.isnan(sst)]
    vmin, vmax = np.percentile(valid_data, [2, 98])
    print(f"Global color scale range: {vmin:.2f}°F to {vmax:.2f}°F")

    zoom_levels = [5, 8, 10]
    for zoom in zoom_levels:
        print(f"\nProcessing zoom level {zoom}")
        if zoom == 5:
            output_sst = sst  # No change for zoom level 5
        elif zoom == 8:
            output_sst = increase_resolution(sst, lat, lon, scale_factor=20)
        elif zoom == 10:
            output_sst = increase_resolution(sst, lat, lon, scale_factor=30)
        
        output_path = os.path.join(output_dir, f'sst_zoom_{zoom}.png')
        save_sst_image(output_sst, output_path, zoom, vmin, vmax)

def main():
    if len(sys.argv) < 2:
        print("Error: NC4 file path not provided")
        sys.exit(1)

    nc4_filepath = sys.argv[1]
    output_dir = './output'
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Processing file: {nc4_filepath}")
    print(f"File exists: {os.path.exists(nc4_filepath)}")
    print(f"File size: {os.path.getsize(nc4_filepath)} bytes")
    
    try:
        sst, lat, lon = load_sst_data(nc4_filepath)
        process_zoom_levels(sst, lat, lon, output_dir)
        print("SST processing completed successfully")
    except Exception as e:
        print(f"Error: SST processing failed - {str(e)}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
