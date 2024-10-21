import xarray as xr
import numpy as np
from PIL import Image
import matplotlib.pyplot as plt
from scipy.ndimage import gaussian_filter
import json

def load_nc4_file(filepath):
    ds = xr.open_dataset(filepath)
    sst = ds['sst'].isel(time=0, level=0).values
    return sst

def celsius_to_fahrenheit(sst):
    return np.where(np.isnan(sst), np.nan, (sst * 9/5) + 32)

def smooth_sst(sst, sigma):
    # Smooth only non-NaN values
    mask = np.isnan(sst)
    sst_smooth = np.copy(sst)
    sst_smooth[~mask] = gaussian_filter(sst[~mask], sigma=sigma, mode='constant', cval=np.nan)
    return sst_smooth

def create_zoom_level(sst, zoom_level):
    if zoom_level == 5:
        return smooth_sst(sst, sigma=1)
    elif zoom_level == 8:
        return smooth_sst(sst, sigma=0.5)
    elif zoom_level == 12:
        return sst  # No smoothing for highest zoom level
    else:
        raise ValueError(f"Unsupported zoom level: {zoom_level}")

def apply_custom_colormap(sst_data, vmin, vmax, colors):
    norm = plt.Normalize(vmin=vmin, vmax=vmax)
    cmap = plt.cm.colors.ListedColormap(colors)
    
    sst_data_masked = np.ma.masked_invalid(sst_data)
    sst_colored = cmap(norm(sst_data_masked))
    sst_image = (sst_colored[:, :, :3] * 255).astype(np.uint8)
    return Image.fromarray(sst_image)

def process_sst_to_images(nc4_filepath, color_scale_filepath, zoom_levels=[5, 8, 12]):
    # Load data
    sst = load_nc4_file(nc4_filepath)
    sst_fahrenheit = celsius_to_fahrenheit(sst)
    
    with open(color_scale_filepath, 'r') as f:
        colors = json.load(f)['colors']

    vmin, vmax = np.nanmin(sst_fahrenheit), np.nanmax(sst_fahrenheit)

    print(f"SST shape: {sst_fahrenheit.shape}")
    print(f"Temperature range: {vmin:.2f}°F to {vmax:.2f}°F")
    print(f"NaN count: {np.isnan(sst_fahrenheit).sum()}")

    for zoom_level in zoom_levels:
        print(f"\nProcessing zoom level {zoom_level}")
        
        zoomed_sst = create_zoom_level(sst_fahrenheit, zoom_level)
        print(f"After processing - NaN count: {np.isnan(zoomed_sst).sum()}")
        
        img = apply_custom_colormap(zoomed_sst, vmin, vmax, colors)
        
        preview_path = f'preview_zoom_{zoom_level}.png'
        img.save(preview_path)
        print(f"Preview for zoom level {zoom_level} saved as {preview_path}")

def main():
    nc4_filepath = './data/capecod.nc4'
    color_scale_filepath = 'color_scale.json'
    process_sst_to_images(nc4_filepath, color_scale_filepath)

if __name__ == "__main__":
    main()