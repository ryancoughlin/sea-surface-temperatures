from pathlib import Path
from datetime import datetime
from typing import NamedTuple, Optional
from config.settings import SOURCES

class AssetPaths(NamedTuple):
    image: Path
    data: Path
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
        # Get the dataset_id from SOURCES config
        dataset_id = SOURCES[dataset]['dataset_id']
        region_name = region.lower().replace(" ", "_")
        date_str = date.strftime('%Y%m%d')
        
        return self.data_dir / f"{dataset_id}_{region_name}_{date_str}.nc"

    def get_asset_paths(self, date: datetime, dataset: str, region: str) -> AssetPaths:
        date_str = date.strftime('%Y%m%d')
        dataset_dir = self.output_dir / region / dataset / date_str
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        return AssetPaths(
            image=dataset_dir / "image.png",
            data=dataset_dir / "data.json",
            contours=dataset_dir / "contours.json" if "contours" in SOURCES[dataset]["supportedLayers"] else None,
            metadata=dataset_dir / "metadata.json"
        )