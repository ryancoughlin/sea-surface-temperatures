import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from pathlib import Path
import sys
import logging
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project root to Python path
root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

from config.regions import REGIONS

# Define consistent styling from generate_thumbnails.py
MAP_STYLE = {
    'figsize': (20, 12),  # Larger figure for overview
    'dpi': 300,
    'land_color': '#EBE59B',
    'ocean_color': '#B1C2D8',
    'state_color': '#4A4A4A',
    'state_linewidth': 0.5,
    'lakes_color': '#B1C2D8',
    'rivers_color': '#B1C2D8',
    'country_color': '#000000',
    'country_linewidth': 1.0,
    'region_color': '#000000',  # Black outlines for regions
    'region_linewidth': 2.0,    # 2px width
}

def visualize_all_regions():
    """Generate a single map showing all region bounds"""
    logger.info("Generating regions overview map")
    
    # Create figure
    fig = plt.figure(figsize=MAP_STYLE['figsize'], dpi=MAP_STYLE['dpi'])
    
    # Set up projection - using Mercator for consistency
    projection = ccrs.Mercator(
        central_longitude=-98.0,  # Roughly center of US
    )
    
    ax = plt.axes(projection=projection)
    
    # Set extent to cover all US regions plus Hawaii
    ax.set_extent([-180, -60, 15, 65], crs=ccrs.PlateCarree())
    
    # Add features with consistent styling
    # Use higher resolution (10m) for more detail
    land = cfeature.NaturalEarthFeature('physical', 'land', '10m',
                                      facecolor=MAP_STYLE['land_color'])
    ocean = cfeature.NaturalEarthFeature('physical', 'ocean', '10m',
                                        facecolor=MAP_STYLE['ocean_color'])
    states = cfeature.NaturalEarthFeature('cultural', 'admin_1_states_provinces_lines', '10m',
                                         edgecolor=MAP_STYLE['state_color'],
                                         facecolor='none',
                                         linewidth=MAP_STYLE['state_linewidth'])
    lakes = cfeature.NaturalEarthFeature('physical', 'lakes', '10m',
                                        facecolor=MAP_STYLE['lakes_color'])
    rivers = cfeature.NaturalEarthFeature('physical', 'rivers_lake_centerlines', '10m',
                                         edgecolor=MAP_STYLE['rivers_color'],
                                         facecolor='none')
    countries = cfeature.NaturalEarthFeature('cultural', 'admin_0_boundary_lines_land', '10m',
                                           edgecolor=MAP_STYLE['country_color'],
                                           facecolor='none',
                                           linewidth=MAP_STYLE['country_linewidth'])
    
    # Add features in correct order
    ax.add_feature(ocean, zorder=1)
    ax.add_feature(land, zorder=2)
    ax.add_feature(lakes, zorder=3)
    ax.add_feature(rivers, linewidth=0.5, zorder=4)
    ax.add_feature(states, zorder=5)
    ax.add_feature(countries, zorder=6)
    
    # Plot region bounds from regions.py
    logger.info("Adding region bounds")
    for region_id, region_data in REGIONS.items():
        bounds = region_data["bounds"]
        lon_min, lat_min = bounds[0]  # Southwest corner
        lon_max, lat_max = bounds[1]  # Northeast corner
        
        logger.info(f"Adding region {region_id}: {bounds}")
        
        # Create rectangle for region bounds
        rect = plt.Rectangle(
            (lon_min, lat_min),
            lon_max - lon_min,
            lat_max - lat_min,
            facecolor='none',
            edgecolor=MAP_STYLE['region_color'],
            linewidth=MAP_STYLE['region_linewidth'],
            transform=ccrs.PlateCarree(),
            zorder=10  # Make sure regions are on top
        )
        ax.add_patch(rect)
    
    # Remove decorations
    ax.spines['geo'].set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])
    
    # Save the map
    output_dir = root_dir / "assets"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "regions_overview.png"
    
    plt.savefig(output_path,
                bbox_inches='tight',
                pad_inches=0,
                dpi=MAP_STYLE['dpi'],
                format='png',
                facecolor='white')
    plt.close(fig)
    
    logger.info(f"Saved regions overview map to {output_path}")

if __name__ == "__main__":
    visualize_all_regions() 