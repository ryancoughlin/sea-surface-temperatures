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
        """Generate tiles for a zoom level."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Get data scale
        valid_data = sst[~np.isnan(sst)]
        vmin, vmax = np.percentile(valid_data, [2, 98])
        print(f"Scale range for zoom {zoom}: {vmin:.2f}°F to {vmax:.2f}°F")
        
        # Generate base image
        fig, ax = plt.subplots(figsize=(10, 12))
        img = ax.imshow(sst, cmap=self.cmap, vmin=vmin, vmax=vmax,
                     extent=[lon.min(), lon.max(), lat.min(), lat.max()])
        
        ax.axis('off')
        plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
        plt.margins(0,0)
        
        output_path = output_dir / f"sst_z{zoom}.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                   pad_inches=0, transparent=True)
        plt.close(fig)
        
        return [output_path]

    def _save_tile(self, sst: np.ndarray, zoom: int, vmin: float, vmax: float) -> Path:
        """Save single tile."""
        fig, ax = plt.subplots(figsize=(10, 12))
        
        ax.imshow(sst, cmap=self.cmap, vmin=vmin, vmax=vmax,
                 extent=[0, sst.shape[1], 0, sst.shape[0]])
        
        ax.axis('off')
        plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
        plt.margins(0,0)
        
        tile_path = settings.TILE_PATH / f"sst_zoom_{zoom}.png"
        plt.savefig(tile_path, dpi=300, bbox_inches='tight', 
                   pad_inches=0, transparent=True)
        plt.close(fig)
        
        return tile_path
