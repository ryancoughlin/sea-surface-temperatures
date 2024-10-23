import numpy as np
from PIL import Image
import matplotlib.pyplot as plt
from pathlib import Path

from ..config.settings import settings

def generate_tiles(sst_data: np.ndarray, zoom_level: int) -> list[Path]:
    """
    Generate map tiles for a zoom level.
    
    Args:
        sst_data: Processed SST data array
        zoom_level: Tile zoom level
        
    Returns:
        List of paths to generated tiles
    """
    tile_paths = []
    settings.TILE_PATH.mkdir(parents=True, exist_ok=True)
    
    # Create colormap
    cmap = plt.cm.RdYlBu_r
    
    # Scale data to 0-255 for PNG
    valid_data = sst_data[~np.isnan(sst_data)]
    vmin, vmax = np.percentile(valid_data, [2, 98])
    normalized = np.clip((sst_data - vmin) / (vmax - vmin), 0, 1)
    
    # Generate base tile
    img_data = (cmap(normalized) * 255).astype(np.uint8)
    img = Image.fromarray(img_data)
    
    # Save base tile
    tile_path = settings.TILE_PATH / f"sst_zoom_{zoom_level}.png"
    img.save(tile_path,