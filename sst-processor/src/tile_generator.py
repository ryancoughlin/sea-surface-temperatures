import json
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
from typing import List, Dict
from .config.settings import settings
from .processors.image_processor import ImageProcessor

class TileGenerator:
    def __init__(self):
        color_path = Path(__file__).parent.parent.parent / "color_scale.json"
        with open(color_path) as f:
            colors = json.load(f)["colors"]
        self.cmap = plt.cm.colors.LinearSegmentedColormap.from_list('custom_cmap', colors)
        self.cmap.set_bad(alpha=0)
        self.image_processor = ImageProcessor()
        
    def generate_tiles(self, sst: np.ndarray, lat: np.ndarray, lon: np.ndarray,
                        zoom: int, source: str, region: str, date: datetime) -> Dict[int, List[Path]]:
        """Generate tiles and full image for a zoom level."""
        tile_paths = {}
        for zoom in settings.ZOOM_LEVELS:
            processed_sst = self.image_processor.process_for_zoom(sst, zoom)
            tile_paths[zoom] = self._generate_tiles_for_zoom(processed_sst, lat, lon, zoom, source, region, date)
        return tile_paths

    def _generate_tiles_for_zoom(self, sst: np.ndarray, lat: np.ndarray, lon: np.ndarray,
                                 zoom: int, source: str, region: str, date: datetime) -> List[Path]:
        date_str = date.strftime('%Y-%m-%d')
        tile_dir = settings.TILE_STRUCTURE['base'] / source / "daily" / date_str / region / f"z{zoom}"
        tile_dir.mkdir(parents=True, exist_ok=True)

        vmin, vmax = np.percentile(sst[~np.isnan(sst)], [2, 98])
        tile_paths = []
        tile_size = settings.TILE_SIZE

        for y in range(0, sst.shape[0], tile_size):
            for x in range(0, sst.shape[1], tile_size):
                tile_data = sst[y:y + tile_size, x:x + tile_size]
                if not np.all(np.isnan(tile_data)):
                    tile_path = tile_dir / f"{x}_{y}.png"
                    self._save_tile(tile_data, vmin, vmax, tile_path)
                    tile_paths.append(tile_path)

        return tile_paths

    def _save_tile(self, data: np.ndarray, vmin: float, vmax: float, path: Path):
        """Save tile with consistent styling."""
        fig, ax = plt.subplots(figsize=(1, 1))
        ax.imshow(data, cmap=self.cmap, vmin=vmin, vmax=vmax)
        ax.axis('off')
        plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
        plt.margins(0,0)
        plt.savefig(path, dpi=256, bbox_inches='tight', pad_inches=0, transparent=True)
        plt.close(fig)

    def generate_all_zoom_levels(self, sst: np.ndarray, lat: np.ndarray, lon: np.ndarray, 
                           source: str, region: str, date: datetime) -> List[Path]:
        """Generate tiles for all zoom levels."""
        tile_paths = []
        for zoom in settings.ZOOM_LEVELS:
            processed_sst = self._process_for_zoom(sst, zoom)
            paths = self.generate_tiles(
                processed_sst, lat, lon, zoom, source, region, date
            )
            tile_paths.extend(paths['tiles'])
        return tile_paths
