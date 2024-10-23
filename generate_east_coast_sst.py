import sys
import os
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import json
from scipy.signal import savgol_filter
import traceback
from pathlib import Path

# Define regions based on the provided JavaScript object
regions = {
    "capecod": {"lat": (39.5, 43.5), "lon": (-71.25, -65.25)},
    "canyonsoverview": {"lat": (36, 42), "lon": (-77, -65)},
    "canyonsnorth": {"lat": (38, 42), "lon": (-74.25, -67)},
    "canyonssouth": {"lat": (36, 40.5), "lon": (-77, -71)},
    "ncarolina": {"lat": (33, 37), "lon": (-79, -72)},
    "gasc": {"lat": (30.5, 34.25), "lon": (-81.75, -75)},
}

def load_sst_data(nc4_filepath):
    print(f"Loading data from {nc4_filepath}")
    try:
        ds = xr.open_dataset(nc4_filepath)
        print(f"Dataset variables: {list(ds.variables)}")
        print(f"SST variable shape: {ds.sst.shape}")
        print(f"Lat shape: {ds.lat.shape}, Lon shape: {ds.lon.shape}")
        
        # Convert to Fahrenheit
        ds['sst'] = ds.sst * 9/5 + 32
        
        print("Fahrenheit SST data stats:")
        print(f"Shape: {ds.sst.shape}")
        print(f"Min: {ds.sst.min().values:.2f}, Max: {ds.sst.max().values:.2f}")
        print(f"NaN count: {np.isnan(ds.sst.values).sum()}")
        
        return ds
    except Exception as e:
        print(f"Error in load_sst_data: {str(e)}")
        traceback.print_exc()
        raise

def extract_region_data(ds, region):
    lat_range, lon_range = regions[region]["lat"], regions[region]["lon"]
    mask = (ds.lat >= lat_range[0]) & (ds.lat <= lat_range[1]) & \
           (ds.lon >= lon_range[0]) & (ds.lon <= lon_range[1])
    region_ds = ds.where(mask, drop=True)
    print(f"Extracted region {region} shape: {region_ds.sst.shape}")
    return region_ds

def smooth_sst_with_savgol(sst, window_length=11, polyorder=2):
    # Apply Savitzky-Golay filter along each axis
    smoothed_sst = savgol_filter(sst, window_length=window_length, polyorder=polyorder, axis=0, mode='nearest')
    smoothed_sst = savgol_filter(smoothed_sst, window_length=window_length, polyorder=polyorder, axis=1, mode='nearest')
    return smoothed_sst

def increase_resolution(sst, scale_factor):
    # Smooth the SST data before interpolation
    smoothed_sst = smooth_sst_with_savgol(sst.values.squeeze(), window_length=11, polyorder=2)
    
    # Use xarray's interp method for interpolation
    new_y = np.linspace(sst.y.min(), sst.y.max(), num=len(sst.y) * scale_factor)
    new_x = np.linspace(sst.x.min(), sst.x.max(), num=len(sst.x) * scale_factor)
    
    high_res_sst = sst.interp(y=new_y, x=new_x, method='linear')
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
    im = ax.pcolormesh(sst.lon, sst.lat, sst.squeeze(), cmap=cmap, vmin=vmin, vmax=vmax)
    
    # Remove all axes, labels, and borders
    ax.axis('off')
    plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
    plt.margins(0,0)
    
    # Save the image without any padding and with transparency
    plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0, transparent=True)
    plt.close(fig)
    print(f"Saved SST image for zoom level {zoom_level} to {output_path}")

def process_zoom_levels(sst, region, output_dir):
    # Calculate global min and max for consistent color scale
    valid_data = sst.values[~np.isnan(sst.values)]
    vmin, vmax = np.percentile(valid_data, [2, 98])
    print(f"Global color scale range: {vmin:.2f}°F to {vmax:.2f}°F")

    zoom_levels = [5, 8, 10]
    for zoom in zoom_levels:
        print(f"\nProcessing zoom level {zoom}")
        if zoom == 5:
            output_sst = sst  # No change for zoom level 5
        elif zoom == 8:
            output_sst = increase_resolution(sst, scale_factor=5)
        elif zoom == 10:
            output_sst = increase_resolution(sst, scale_factor=10)
        
        output_path = output_dir / f'{region}_sst_zoom_{zoom}.png'
        save_sst_image(output_sst, output_path, zoom, vmin, vmax)

def main(nc4_file_path, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        ds = load_sst_data(nc4_file_path)
        
        for region in regions:
            print(f"\nProcessing {region}...")
            region_ds = extract_region_data(ds, region)
            process_zoom_levels(region_ds.sst, region, output_dir)
        
        print("SST processing completed successfully")
    except Exception as e:
        print(f"Error: SST processing failed - {str(e)}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: NC4 file path not provided")
        sys.exit(1)

    nc4_file_path = sys.argv[1]
    output_dir = "output"
    main(nc4_file_path, output_dir)
