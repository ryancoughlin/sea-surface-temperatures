import matplotlib.pyplot as plt
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from config.settings import IMAGE_SETTINGS, REGIONS_DIR  # Direct imports

logger = logging.getLogger(__name__)

class BaseImageProcessor(ABC):
    """Base class for all image processors."""
    def __init__(self):
        self.settings = IMAGE_SETTINGS
        self.base_dir = REGIONS_DIR

    @abstractmethod
    def generate_image(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Each processor must implement this."""
        pass

    def generate_image_path(self, region: str, dataset: str, timestamp: str) -> Path:
        """Generate standardized path for image storage."""
        path = self.base_dir / region / "datasets" / dataset / timestamp / "image.png"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def save_image(self, fig, region: str, dataset: str, timestamp: str) -> Path:
        """Save figure to standardized location."""
        try:
            logger.info(f"Figure type: {type(fig)}")
            image_path = self.generate_image_path(region, dataset, timestamp)
            
            if not hasattr(fig, 'savefig'):
                raise ValueError(f"Expected matplotlib figure, got {type(fig)}")
                
            fig.savefig(image_path, dpi=self.settings['dpi'], bbox_inches='tight')
            plt.close(fig)
            return image_path
        except Exception as e:
            logger.error(f"Error in save_image: {str(e)}")
            raise
