from pathlib import Path
from datetime import datetime
from typing import NamedTuple, Optional
from config.settings import SOURCES

class AssetPaths(NamedTuple):
    image: Path
    contours: Optional[Path]
    metadata: Path
class PathManager:
    BASE_DIR = Path(__file__).parent.parent

    def __init__(self):
        self.base_dir = self.BASE_DIR
        self.data_dir = self.base_dir / "data"
        self.output_dir = self.base_dir / "output"

    def ensure_directories(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_data_path(self, date: datetime, dataset: str, region: str) -> Path:
        return self.data_dir / f"{dataset}_{region}_{date.strftime('%Y%m%d')}.nc"

    def get_asset_paths(self, date: datetime, dataset: str, region: str) -> AssetPaths:
        date_str = date.strftime('%Y%m%d')
        dataset_dir = self.output_dir / region / dataset / date_str
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        return AssetPaths(
            image=dataset_dir / "image.png",
            contours=dataset_dir / "contours.geojson" if "contours" in SOURCES[dataset]["supportedLayers"] else None,
            metadata=dataset_dir / "metadata.json"
        )