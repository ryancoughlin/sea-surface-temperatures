from pathlib import Path
import numpy as np
from PIL import Image
from config.settings import TILE_SETTINGS, REGIONS_DIR  # Direct imports

class TileGenerator:
    def __init__(self):
        self.tile_size = TILE_SETTINGS['tile_size']
        self.zoom_levels = TILE_SETTINGS['zoom_levels']
        self.base_dir = REGIONS_DIR
    
    def generate_tiles(self, image: Image.Image, region: str, dataset: str, timestamp: str) -> None:
        """Generate tiles from a PIL Image object for specified zoom levels."""
        output_dir = self.base_dir / region / "datasets" / dataset / timestamp / "tiles"
        width, height = image.size
        
        for zoom in self.zoom_levels:
            num_tiles_x = int(np.ceil(width / self.tile_size))
            num_tiles_y = int(np.ceil(height / self.tile_size))
            
            for x in range(num_tiles_x):
                for y in range(num_tiles_y):
                    left = x * self.tile_size
                    upper = y * self.tile_size
                    right = min((x + 1) * self.tile_size, width)
                    lower = min((y + 1) * self.tile_size, height)
                    
                    tile = image.crop((left, upper, right, lower))
                    
                    tile_dir = output_dir / f"{zoom}" / f"{x}"
                    tile_dir.mkdir(parents=True, exist_ok=True)
                    tile_path = tile_dir / f"{y}.png"
                    tile.save(tile_path)
