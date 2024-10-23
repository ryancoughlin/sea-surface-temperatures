import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
from typing import Tuple, Dict
from .config.settings import settings
from .processors.image_processor import ImageProcessor

class FullImageGenerator:
    def __init__(self):
        self.image_processor = ImageProcessor()

    def generate_full_image(self, sst: np.ndarray, lat: np.ndarray, lon: np.ndarray,
                             zoom: int, source: str, region: str, date: datetime) -> Dict[int, Path]:
        full_image_paths = {}
        for zoom in settings.ZOOM_LEVELS:
            processed_sst = self.image_processor.process_for_zoom(sst, zoom)
            full_image_paths[zoom] = self._generate_full_image_for_zoom(processed_sst, lat, lon, zoom, source, region, date)
        return full_image_paths

    def _generate_full_image_for_zoom(self, sst: np.ndarray, lat: np.ndarray, lon: np.ndarray,
                                      zoom: int, source: str, region: str, date: datetime) -> Path:
        date_str = date.strftime('%Y-%m-%d')
        image_path = settings.TILE_STRUCTURE['base'] / source / "daily" / date_str / region
        image_path.mkdir(parents=True, exist_ok=True)
        output_path = image_path / f"full_z{zoom}.png"

        vmin, vmax = np.percentile(sst[~np.isnan(sst)], [2, 98])
        extent = [lon.min(), lon.max(), lat.min(), lat.max()]

        self._save_full_image(sst, vmin, vmax, output_path, extent)
        return output_path

    def _save_full_image(self, data: np.ndarray, vmin: float, vmax: float, path: Path, extent):
        plt.figure(figsize=(10, 10))
        plt.imshow(data, cmap='viridis', vmin=vmin, vmax=vmax, extent=extent)
        plt.colorbar(label='Sea Surface Temperature (°C)')
        plt.title(f'SST Map - {path.stem}')
        plt.axis('off')
        plt.savefig(path, dpi=300, bbox_inches='tight')
        plt.close()
