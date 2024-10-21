import rasterio
import numpy as np
from PIL import Image
import matplotlib.pyplot as plt
import json
from pykrige.ok import OrdinaryKriging
from scipy.ndimage import gaussian_filter
import xarray as xr

def load_nc4_file(filepath):
    with xr.open_dataset(filepath) as ds:
        sst = ds['sst'].isel(time=0, level=0).values
        lats = ds['lat'].values
        lons = ds['lon'].values
    
    # Create a transform based on the lat/lon coordinates
    transform = rasterio.transform.from_bounds(
        lons.min(), lats.min(), lons.max(), lats.max(), sst.shape[1], sst.shape[0]
    )
    
    return sst, transform

def celsius_to_fahrenheit(sst):
    return np.where(np.isnan(sst), np.nan, (sst * 9/5) + 32)

def apply_custom_colormap(sst_data, vmin, vmax, colors):
    norm = plt.Normalize(vmin=vmin, vmax=vmax)
    cmap = plt.cm.colors.ListedColormap(colors)
    
    sst_data_masked = np.ma.masked_invalid(sst_data)
    sst_colored = cmap(norm(sst_data_masked))
    sst_image = (sst_colored[:, :, :3] * 255).astype(np.uint8)
    return Image.fromarray(sst_image)

def enhance_sst(sst, transform, zoom_level):
    if zoom_level == 5:
        zoom_factor = 1
        smooth_sigma = 0.5
    elif zoom_level == 8:
        zoom_factor = 4
        smooth_sigma = 1.0
    elif zoom_level == 10:
        zoom_factor = 8
        smooth_sigma = 1.5
    else:
        raise ValueError(f"Unsupported zoom level: {zoom_level}")

    print(f"Zoom factor: {zoom_factor}")
    print(f"Smoothing sigma: {smooth_sigma:.2f}")

    # Create a grid of the original data points
    ny, nx = sst.shape
    x = np.arange(0, nx)
    y = np.arange(0, ny)
    X, Y = np.meshgrid(x, y)

    # Flatten the arrays
    X_flat = X.flatten()
    Y_flat = Y.flatten()
    sst_flat = sst.flatten()

    # Remove NaN values
    mask = ~np.isnan(sst_flat)
    X_valid = X_flat[mask]
    Y_valid = Y_flat[mask]
    sst_valid = sst_flat[mask]

    # Create a grid for interpolation
    grid_x = np.linspace(0, nx - 1, nx * zoom_factor)
    grid_y = np.linspace(0, ny - 1, ny * zoom_factor)

    # Perform Ordinary Kriging
    ok = OrdinaryKriging(
        X_valid,
        Y_valid,
        sst_valid,
        variogram_model="gaussian",
        verbose=False,
        enable_plotting=False,
    )
    sst_interpolated, _ = ok.execute("grid", grid_x, grid_y)

    # Apply smoothing
    sst_smooth = gaussian_filter(sst_interpolated, sigma=smooth_sigma)

    # Update the transform for the new resolution
    new_transform = rasterio.Affine(
        transform.a / zoom_factor,
        transform.b,
        transform.c,
        transform.d,
        transform.e / zoom_factor,
        transform.f
    )

    return sst_smooth, new_transform

def process_sst_to_images(nc4_filepath, color_scale_filepath, zoom_levels=[5, 8, 10]):
    sst, transform = load_nc4_file(nc4_filepath)
    sst_fahrenheit = celsius_to_fahrenheit(sst)
    
    with open(color_scale_filepath, 'r') as f:
        colors = json.load(f)['colors']

    vmin, vmax = np.nanmin(sst_fahrenheit), np.nanmax(sst_fahrenheit)
    print(f"Original temperature range: {vmin:.2f}°F to {vmax:.2f}°F")

    # Adjust vmin and vmax to reasonable values if needed
    vmin = max(vmin, 30)  # Assuming 30°F as a reasonable minimum
    vmax = min(vmax, 85)  # Assuming 85°F as a reasonable maximum
    print(f"Adjusted temperature range: {vmin:.2f}°F to {vmax:.2f}°F")

    print(f"SST shape: {sst_fahrenheit.shape}")
    print(f"NaN count: {np.isnan(sst_fahrenheit).sum()}")

    for zoom_level in zoom_levels:
        print(f"\nProcessing zoom level {zoom_level}")
        
        sst_enhanced, new_transform = enhance_sst(sst_fahrenheit, transform, zoom_level)
        
        img = apply_custom_colormap(sst_enhanced, vmin, vmax, colors)
        
        preview_path = f'./public/capecod_sst_{zoom_level}.png'
        img.save(preview_path)
        print(f"Preview for zoom level {zoom_level} saved as {preview_path}")
        print(f"Image size: {img.size}")
        
        # Save as GeoTIFF
        geotiff_path = f'./public/capecod_sst_{zoom_level}.tif'
        with rasterio.open(
            geotiff_path,
            'w',
            driver='GTiff',
            height=sst_enhanced.shape[0],
            width=sst_enhanced.shape[1],
            count=1,
            dtype=sst_enhanced.dtype,
            crs=rasterio.crs.CRS.from_epsg(4326),  # Assuming WGS84
            transform=new_transform,
        ) as dst:
            dst.write(sst_enhanced, 1)
        print(f"GeoTIFF for zoom level {zoom_level} saved as {geotiff_path}")
        
        valid_temps = sst_enhanced[~np.isnan(sst_enhanced)]
        if valid_temps.size > 0:
            print(f"Processed temperature range: {np.min(valid_temps):.2f}°F to {np.max(valid_temps):.2f}°F")
        else:
            print("Warning: No valid temperature data after processing")

        del sst_enhanced
        del img

def main():
    nc4_filepath = './data/capecod.nc4'
    color_scale_filepath = 'color_scale.json'
    process_sst_to_images(nc4_filepath, color_scale_filepath)

if __name__ == "__main__":
    main()