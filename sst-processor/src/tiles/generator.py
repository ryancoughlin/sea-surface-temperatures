import numpy as np
import rasterio
import matplotlib.pyplot as plt
import json
from pathlib import Path
from ..config.settings import settings

class TileGenerator:
    def __init__(self):
        with open('color_scale.json', 'r') as f:
            color_scale = json.load(f)
        self.colors = color_scale['colors']
        self.cmap = plt.cm.colors.LinearSegmentedColormap.from_list('custom_cmap', self.colors)
        self.cmap.set_bad(alpha=0)
        
    def generate_tiles(self, sst: np.ndarray, lat: np.ndarray, lon: np.ndarray, 
                      zoom: int, output_dir: Path) -> list[Path]:
        """Generate full image and tiles for a zoom level."""
        output_dir.mkdir(parents=True, exist_ok=True)
        tile_paths = []
        
        # Get data scale
        valid_data = sst[~np.isnan(sst)]
        vmin, vmax = np.percentile(valid_data, [2, 98])
        print(f"Scale range for zoom {zoom}: {vmin:.2f}°F to {vmax:.2f}°F")
        
        # Generate and save full image
        full_image_path = self._save_full_image(sst, lat, lon, zoom, output_dir, vmin, vmax)
        tile_paths.append(full_image_path)
        
        # Generate tiles
        tile_size = settings.TILE_SIZE
        for y in range(0, sst.shape[0], tile_size):
            for x in range(0, sst.shape[1], tile_size):
                # Extract tile data
                tile_data = sst[y:y + tile_size, x:x + tile_size]
                if tile_data.size > 0:  # Only process non-empty tiles
                    tile_path = self._save_tile(tile_data, zoom, y//tile_size, x//tile_size, vmin, vmax, output_dir)
                    tile_paths.append(tile_path)
        
        return tile_paths

    def _save_full_image(self, sst: np.ndarray, lat: np.ndarray, lon: np.ndarray,
                        zoom: int, output_dir: Path, vmin: float, vmax: float) -> Path:
        """Save full resolution image."""
        fig, ax = plt.subplots(figsize=(10, 12))
        img = ax.imshow(sst, cmap=self.cmap, vmin=vmin, vmax=vmax,
                     extent=[lon.min(), lon.max(), lat.min(), lat.max()])
        
        ax.axis('off')
        plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
        plt.margins(0,0)
        
        output_path = output_dir / f"sst_z{zoom}_full.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                   pad_inches=0, transparent=True)
        plt.close(fig)
        
        return output_path

    def _save_tile(self, sst: np.ndarray, zoom: int, y: int, x: int, 
                   vmin: float, vmax: float, output_dir: Path) -> Path:
        """Save individual tile."""
        fig, ax = plt.subplots(figsize=(10, 10))
        ax.imshow(sst, cmap=self.cmap, vmin=vmin, vmax=vmax)
        
        ax.axis('off')
        plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
        plt.margins(0,0)
        
        tile_path = output_dir / f"sst_z{zoom}_y{y}_x{x}.png"
        plt.savefig(tile_path, dpi=300, bbox_inches='tight', 
                   pad_inches=0, transparent=True)
        plt.close(fig)
        
        return tile_path
