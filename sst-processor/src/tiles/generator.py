import numpy as np
import matplotlib.pyplot as plt
import json
from pathlib import Path
from ..config.settings import settings
from datetime import datetime
from typing import List

class TileGenerator:
    def __init__(self):
        with open('color_scale.json', 'r') as f:
            color_scale = json.load(f)
        self.colors = color_scale['colors']
        self.cmap = plt.cm.colors.LinearSegmentedColormap.from_list('custom_cmap', self.colors)
        self.cmap.set_bad(alpha=0)
        
    def generate_tiles(self, sst: np.ndarray, lat: np.ndarray, lon: np.ndarray, 
                      zoom: int, source: str, region: str, date: datetime) -> List[Path]:
        """Generate tiles using organized structure."""
        source_config = settings.TILE_STRUCTURE['sources'][source]
        date_str = date.strftime('%Y-%m-%d')
        
        # Build path: source/frequency/date/region/zoom
        tile_dir = (settings.TILE_STRUCTURE['base'] 
                    / source_config['path']
                    / source_config['update_frequency']
                    / date_str
                    / region.value
                    / f'z{zoom}')
        
        tile_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate and save tiles
        valid_data = sst[~np.isnan(sst)]
        vmin, vmax = np.percentile(valid_data, [2, 98])
        print(f"Scale range for zoom {zoom}: {vmin:.2f}°F to {vmax:.2f}°F")
        
        tile_paths = []
        tile_size = 256
        
        # Save full resolution image first
        full_path = tile_dir / f"full.png"
        self._save_image(sst, vmin, vmax, full_path, extent=[lon.min(), lon.max(), lat.min(), lat.max()])
        tile_paths.append(full_path)
        
        # Generate individual tiles
        for y in range(0, sst.shape[0], tile_size):
            for x in range(0, sst.shape[1], tile_size):
                tile_data = sst[y:y + tile_size, x:x + tile_size]
                if not np.all(np.isnan(tile_data)):
                    tile_path = tile_dir / f"{x}_{y}.png"
                    self._save_image(tile_data, vmin, vmax, tile_path)
                    tile_paths.append(tile_path)
        
        # Update latest symlink
        latest_link = (settings.TILE_STRUCTURE['base'] 
                      / source_config['path']
                      / source_config['update_frequency']
                      / 'latest')
        if latest_link.exists():
            latest_link.unlink()
        latest_link.symlink_to(date_str)
        
        return tile_paths

    def _save_image(self, data: np.ndarray, vmin: float, vmax: float, 
                   path: Path, extent=None) -> None:
        """Save image with consistent styling."""
        fig, ax = plt.subplots(figsize=(10, 10))
        if extent:
            ax.imshow(data, cmap=self.cmap, vmin=vmin, vmax=vmax, extent=extent)
        else:
            ax.imshow(data, cmap=self.cmap, vmin=vmin, vmax=vmax)
        
        ax.axis('off')
        plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
        plt.margins(0,0)
        plt.savefig(path, dpi=300, bbox_inches='tight', pad_inches=0, transparent=True)
        plt.close(fig)
