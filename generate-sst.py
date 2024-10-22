import os
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import cmocean

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

def increase_resolution(sst, scale_factor):
    # Create higher resolution grid
    new_shape = (sst.shape[0] * scale_factor, sst.shape[1] * scale_factor)
    high_res_sst = np.full(new_shape, np.nan)
    
    # Fill in the high resolution grid with original data
    for i in range(scale_factor):
        for j in range(scale_factor):
            high_res_sst[i::scale_factor, j::scale_factor] = sst
    
    return high_res_sst

def save_sst_image(sst, output_path, zoom_level, vmin, vmax):
    fig, ax = plt.subplots(figsize=(10, 12))
    
    # Create a masked array to properly handle NaN values
    masked_sst = np.ma.masked_invalid(sst)
    
    # Use a custom colormap with transparency for NaN values
    cmap = cmocean.cm.thermal.copy()
    cmap.set_bad(alpha=0)  # Set NaN values to be fully transparent
    
    im = ax.imshow(masked_sst, cmap=cmap, interpolation='nearest', vmin=vmin, vmax=vmax, 
                   extent=[0, sst.shape[1], 0, sst.shape[0]])
    
    cbar = plt.colorbar(im, ax=ax, label='Temperature (°F)')
    cbar.ax.tick_params(labelsize=8)
    ax.set_title(f'Sea Surface Temperature - Zoom Level {zoom_level}')
    ax.axis('off')
    plt.tight_layout()
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
            output_sst = increase_resolution(sst, scale_factor=2)
        elif zoom == 10:
            output_sst = increase_resolution(sst, scale_factor=4)
        
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
