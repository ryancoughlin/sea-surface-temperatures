import os
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from scipy.ndimage import gaussian_filter
import cmocean

def kelvin_to_fahrenheit(temp_k):
    return (temp_k - 273.15) * 9/5 + 32

def celsius_to_fahrenheit(temp_c):
    return temp_c * 9/5 + 32

def load_sst_data(nc4_filepath):
    with xr.open_dataset(nc4_filepath) as ds:
        sst = ds.sst.squeeze().values  # Remove singleton dimensions
        lat = ds.lat.values
        lon = ds.lon.values
    
    print("Raw SST data stats:")
    print(f"Shape: {sst.shape}")
    print(f"Min: {np.nanmin(sst):.2f}, Max: {np.nanmax(sst):.2f}")
    print(f"NaN count: {np.isnan(sst).sum()}")
    
    # Check if data is already in Fahrenheit
    if np.nanmax(sst) > 100:
        print("Data appears to be in Fahrenheit already")
        sst_fahrenheit = sst
    else:
        # Convert to Fahrenheit
        sst_fahrenheit = (sst * 9/5) + 32
    
    print("Fahrenheit SST data stats:")
    print(f"Min: {np.nanmin(sst_fahrenheit):.2f}, Max: {np.nanmax(sst_fahrenheit):.2f}")
    
    return sst_fahrenheit, lat, lon

def apply_smoothing(sst, sigma):
    # Apply smoothing only to non-NaN values
    mask = np.isnan(sst)
    smoothed = gaussian_filter(np.where(mask, 0, sst), sigma=sigma, mode='nearest')
    correction = gaussian_filter(np.where(mask, 0, 1), sigma=sigma, mode='nearest')
    smoothed_sst = np.where(mask, np.nan, smoothed / (correction + 1e-8))
    return smoothed_sst

def save_sst_image(sst, lat, lon, output_path, zoom_level):
    valid_data = sst[~np.isnan(sst)]
    vmin, vmax = np.percentile(valid_data, [2, 98])
    
    fig, ax = plt.subplots(figsize=(10, 12))
    im = ax.imshow(sst, cmap=cmocean.cm.thermal, interpolation='nearest', 
                   vmin=vmin, vmax=vmax, extent=[lon.min(), lon.max(), lat.min(), lat.max()])
    cbar = plt.colorbar(im, ax=ax, label='Temperature (°F)')
    cbar.ax.tick_params(labelsize=8)
    ax.set_title(f'Sea Surface Temperature - Zoom Level {zoom_level}')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved SST image for zoom level {zoom_level} to {output_path}")
    print(f"Temperature range: {vmin:.2f}°F to {vmax:.2f}°F")

def process_zoom_level(sst, lat, lon, zoom_level, output_dir):
    if zoom_level == 5:
        smoothed_sst = apply_smoothing(sst, sigma=1)
    elif zoom_level == 8:
        smoothed_sst = apply_smoothing(sst, sigma=0.5)
    elif zoom_level == 10:
        smoothed_sst = apply_smoothing(sst, sigma=0.3)
    else:
        raise ValueError(f"Unsupported zoom level: {zoom_level}")

    output_path = os.path.join(output_dir, f'sst_zoom_{zoom_level}.png')
    save_sst_image(smoothed_sst, lat, lon, output_path, zoom_level)

def main():
    nc4_filepath = './data/capecod.nc4'
    output_dir = './output'
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        sst, lat, lon = load_sst_data(nc4_filepath)
        print(f"SST data loaded. Shape: {sst.shape}")
        print(f"Temperature range: {np.nanmin(sst):.2f}°F to {np.nanmax(sst):.2f}°F")
        
        zoom_levels = [5, 8, 10]
        for zoom in zoom_levels:
            process_zoom_level(sst, lat, lon, zoom, output_dir)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
