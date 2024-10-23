import os
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import RegularGridInterpolator
import json
from scipy.signal import savgol_filter

def load_sst_data(nc4_filepath):
    with xr.open_dataset(nc4_filepath) as ds:
        # Try to load 'sst' or 'analysed_sst'
        if 'sst' in ds:
            sst = ds.sst.squeeze().values
        elif 'analysed_sst' in ds:
            sst = ds.analysed_sst.squeeze().values
        else:
            raise ValueError("Neither 'sst' nor 'analysed_sst' found in the dataset")
        
        lat = ds.lat.values
        lon = ds.lon.values
    
    # Convert to Fahrenheit
    sst_fahrenheit = (sst * 9/5) + 32
    
    print("Fahrenheit SST data stats:")
    print(f"Shape: {sst_fahrenheit.shape}")
    print(f"Min: {np.nanmin(sst_fahrenheit):.2f}, Max: {np.nanmax(sst_fahrenheit):.2f}")
    print(f"NaN count: {np.isnan(sst_fahrenheit).sum()}")
    
    return sst_fahrenheit, lat, lon

def smooth_sst_with_savgol(sst, window_length=11, polyorder=2):
    # Apply Savitzky-Golay filter along each axis
    smoothed_sst = savgol_filter(sst, window_length=window_length, polyorder=polyorder, axis=0, mode='nearest')
    smoothed_sst = savgol_filter(smoothed_sst, window_length=window_length, polyorder=polyorder, axis=1, mode='nearest')
    return smoothed_sst

def interpolate_sst(sst, scale_factor):
    # Create a mask for valid data points
    mask = ~np.isnan(sst)
    y, x = np.indices(sst.shape)
    
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
    
    # Round to eight decimal points for more detail
    interpolated_sst = np.round(interpolated_sst, 8)
    
    return interpolated_sst

def increase_resolution(sst, scale_factor, smooth=False):
    if smooth:
        # Smooth the SST data before interpolation
        smoothed_sst = smooth_sst_with_savgol(sst, window_length=11, polyorder=2)
        return interpolate_sst(smoothed_sst, scale_factor)
    else:
        # Use nearest-neighbor interpolation to create a pixelated effect
        rows, cols = sst.shape
        new_rows, new_cols = rows * scale_factor, cols * scale_factor
        
        # Create indices for the new grid
        row_indices = np.repeat(np.arange(rows), scale_factor)
        col_indices = np.repeat(np.arange(cols), scale_factor)
        
        # Use advanced indexing to create the high-resolution grid
        high_res_sst = sst[row_indices[:, np.newaxis], col_indices]
        
        return high_res_sst

def save_sst_image(sst, output_path, zoom_level, vmin, vmax):
    fig, ax = plt.subplots(figsize=(10, 12))
    
    with open('color_scale.json', 'r') as f:
        color_scale = json.load(f)
    colors = color_scale['colors']
    cmap = plt.cm.colors.LinearSegmentedColormap.from_list('custom_cmap', colors)
    
    # Set NaN values to be fully transparent in the colormap
    cmap.set_bad(alpha=0)
    
    # Plot the SST data using imshow with nearest interpolation
    ax.imshow(sst, cmap=cmap, vmin=vmin, vmax=vmax, 
              extent=[0, sst.shape[1], 0, sst.shape[0]],
              interpolation='nearest')  # Use nearest neighbor interpolation
    
    # Remove all axes, labels, and borders
    ax.axis('off')
    plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
    plt.margins(0,0)
    
    # Save the image without any padding and with transparency
    plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0, transparent=True)
    plt.close(fig)
    print(f"Saved SST image for zoom level {zoom_level} to {output_path}")

def process_zoom_levels(sst, lat, lon, output_dir, smooth=False):
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
            output_sst = increase_resolution(sst, scale_factor=4, smooth=smooth)
        elif zoom == 10:
            output_sst = increase_resolution(sst, scale_factor=20, smooth=smooth)
        
        suffix = "_smooth" if smooth else "_pixelated"
        output_path = os.path.join(output_dir, f'sst_zoom_{zoom}{suffix}.png')
        save_sst_image(output_sst, output_path, zoom, vmin, vmax)

def main():
    nc4_filepath = './data/eastcoast-geopolar.nc4'
    output_dir = './output'
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        sst, lat, lon = load_sst_data(nc4_filepath)
        
        # Process both smooth and pixelated versions
        process_zoom_levels(sst, lat, lon, output_dir, smooth=True)
        process_zoom_levels(sst, lat, lon, output_dir, smooth=False)
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
