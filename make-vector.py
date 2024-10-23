import numpy as np
import xarray as xr
import geopandas as gpd
from shapely.geometry import LineString
from scipy.ndimage import gaussian_filter
import matplotlib.pyplot as plt
import os

def load_sst_data(nc4_filepath, downsample_factor=2):
    """Load and prepare SST data with downsampling"""
    print("Loading and downsampling data...")
    with xr.open_dataset(nc4_filepath) as ds:
        sst = ds.sst.squeeze().values[::downsample_factor, ::downsample_factor]
        lats = ds.lat.values[::downsample_factor]
        lons = ds.lon.values[::downsample_factor]
    
    # Convert to Fahrenheit
    sst = (sst * 9/5) + 32
    
    # Apply light smoothing to reduce noise while preserving significant gradients
    # Only smooth valid (non-NaN) areas
    mask = ~np.isnan(sst)
    sst_smooth = np.copy(sst)
    sst_smooth[mask] = gaussian_filter(sst[mask], sigma=1)
    
    print(f"Downsampled shape: {sst.shape}")
    print(f"Temperature range: {np.nanmin(sst):.2f}°F to {np.nanmax(sst):.2f}°F")
    print(f"Valid data points: {np.sum(mask)}")
    
    return sst_smooth, lats, lons

def define_isotherm_levels(sst, interval=2.0, fixed_range=None):
    """Define isotherm levels with custom ranges"""
    if fixed_range:
        vmin, vmax = fixed_range
    else:
        # Get the actual data range using percentiles to avoid outliers
        vmin, vmax = np.nanpercentile(sst, [2, 98])
        # Round to nearest interval
        vmin = np.floor(vmin / interval) * interval
        vmax = np.ceil(vmax / interval) * interval
    
    # Create contour levels
    levels = np.arange(vmin, vmax + interval, interval)
    
    print(f"\nIsotherm Configuration:")
    print(f"Temperature range: {vmin:.1f}°F to {vmax:.1f}°F")
    print(f"Interval: {interval}°F")
    print(f"Generating isotherms at: {levels}")
    
    return levels

def generate_isotherms(sst, lats, lons, interval=2.0):
    """Generate isotherms at specified intervals"""
    print("\nGenerating isotherms...")
    
    # Define contour levels
    levels = define_isotherm_levels(sst, interval=interval)
    
    # Create coordinate meshgrid
    lon_grid, lat_grid = np.meshgrid(lons, lats)
    
    # Generate contours using matplotlib
    fig, ax = plt.subplots()
    sst_masked = np.ma.masked_array(sst, mask=np.isnan(sst))
    cs = ax.contour(lon_grid, lat_grid, sst_masked, levels=levels)
    plt.close(fig)
    
    geometries = []
    temperatures = []
    
    # Extract contour paths
    for i, level in enumerate(cs.levels):
        paths = cs.collections[i].get_paths()
        for path in paths:
            vertices = path.vertices
            if len(vertices) >= 20:  # Filter out very short contours
                try:
                    line = LineString(vertices)
                    if line.is_valid and not line.is_empty and line.length > 0.01:
                        geometries.append(line)
                        temperatures.append(float(level))
                except Exception as e:
                    print(f"Skipping invalid contour: {str(e)[:100]}")
    
    print(f"Generated {len(geometries)} valid isotherms")
    return geometries, temperatures

def create_shapefile(geometries, temperatures, output_path):
    """Create shapefile from isotherm geometries"""
    if not geometries:
        print("No valid isotherms to save!")
        return None
    
    print(f"\nCreating shapefile...")
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame({
        'temp_f': temperatures,
        'geometry': geometries
    })
    
    # Set CRS to WGS 84
    gdf.set_crs(epsg=4326, inplace=True)
    
    # Save as shapefile
    gdf.to_file(output_path, driver='ESRI Shapefile')
    print(f"Saved shapefile to {output_path}")
    
    return gdf

def create_preview(gdf, sst, lats, lons, output_path):
    """Create a preview plot of the isotherms over SST data"""
    if gdf is None or gdf.empty:
        return
    
    print("\nCreating preview plot...")
    fig, ax = plt.subplots(figsize=(15, 15))
    
    # Plot base SST data
    lon_grid, lat_grid = np.meshgrid(lons, lats)
    sst_masked = np.ma.masked_array(sst, mask=np.isnan(sst))
    plt.pcolormesh(lon_grid, lat_grid, sst_masked, cmap='coolwarm', alpha=0.5)
    plt.colorbar(label='Temperature (°F)')
    
    # Plot isotherms
    gdf.plot(ax=ax, color='black', linewidth=1.5)
    
    # Add temperature labels
    for idx, row in gdf.iterrows():
        # Get the midpoint of the line for label placement
        mid_point = row.geometry.interpolate(0.5, normalized=True)
        plt.annotate(f"{row['temp_f']:.0f}°F", 
                    (mid_point.x, mid_point.y),
                    xytext=(3, 3), textcoords='offset points',
                    fontsize=8, color='black',
                    bbox=dict(facecolor='white', alpha=0.7, edgecolor='none'))
    
    ax.set_title('Sea Surface Temperature Isotherms')
    
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved preview to {output_path}")

def main():
    if len(os.sys.argv) < 2:
        print("Usage: python script.py <path_to_nc4_file>")
        os.sys.exit(1)
    
    nc4_filepath = os.sys.argv[1]
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Load data
    sst, lats, lons = load_sst_data(nc4_filepath, downsample_factor=2)
    
    # Generate isotherms
    geometries, temperatures = generate_isotherms(sst, lats, lons, interval=2.0)
    
    # Create shapefile
    output_path = os.path.join(output_dir, "sst_isotherms")
    gdf = create_shapefile(geometries, temperatures, output_path)
    
    # Create preview
    if gdf is not None:
        preview_path = os.path.join(output_dir, "sst_preview.png")
        create_preview(gdf, sst, lats, lons, preview_path)

if __name__ == "__main__":
    main()