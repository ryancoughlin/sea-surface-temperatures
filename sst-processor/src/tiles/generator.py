import numpy as np
import matplotlib.pyplot as plt
import json
from pathlib import Path

from ..config.settings import settings
from ..data.processor import smooth_and_interpolate

class TileGenerator:
    def __init__(self):
        with open('color_scale.json', 'r') as f:
            color_scale = json.load(f)
        self.colors = color_scale['colors']
        self.cmap = plt.cm.colors.LinearSegmentedColormap.from_list('custom_cmap', self.colors)
        self.cmap.set_bad(alpha=0)

    def generate_tiles(self, sst: np.ndarray, lat: np.ndarray, lon: np.ndarray) -> list[Path]:
        """Generate tiles for all zoom levels."""
        settings.TILE_PATH.mkdir(parents=True, exist_ok=True)
        
        # Get global scale
        valid_data = sst[~np.isnan(sst)]
        vmin, vmax = np.percentile(valid_data, [2, 98])
        
        tile_paths = []
        for zoom in settings.ZOOM_LEVELS:
            if zoom == 5:
                output_sst = sst
            elif zoom == 8:
                output_sst = smooth_and_interpolate(sst, scale_factor=20)
            elif zoom == 10:
                output_sst = smooth_and_interpolate(sst, scale_factor=30)
                
            path = self._save_tile(output_sst, zoom, vmin, vmax)
            tile_paths.append(path)
            
        return tile_paths
    
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
