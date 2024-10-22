import os
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import cmocean
import scipy.ndimage
import scipy.interpolate
from scipy.interpolate import RectBivariateSpline
from scipy.interpolate import griddata
from scipy.interpolate import RegularGridInterpolator
from scipy.interpolate import LinearNDInterpolator
from scipy.interpolate import NearestNDInterpolator
import gc

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

def increase_resolution(sst, lat, lon, scale_factor):
    high_res_sst = bilinear_interpolate(sst, scale_factor)
    
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
    
    # Define a custom color gradient
    colors = ['#9b0bac', '#4d4dff', '#0080ff', '#00ffff', '#00ff80', '#80ff00', '#ffff00', '#ffbf00', '#ff8000', '#ff4000', '#ff0000', '#800000', '#400000']
    n_bins = 100  # Increased color gradations for smoother transition
    cmap = plt.cm.colors.LinearSegmentedColormap.from_list('custom_cmap', colors, N=n_bins)
    
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
            output_sst = increase_resolution(sst, lat, lon, scale_factor=2)
        elif zoom == 10:
            output_sst = increase_resolution(sst, lat, lon, scale_factor=4)
        
        output_path = os.path.join(output_dir, f'sst_zoom_{zoom}.png')
        save_sst_image(output_sst, output_path, zoom, vmin, vmax)

def main():
    nc4_filepath = './data/capecod.nc4'
    output_dir = './output'
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        sst, lat, lon = load_sst_data(nc4_filepath)
        process_zoom_levels(sst, lat, lon, output_dir)
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
