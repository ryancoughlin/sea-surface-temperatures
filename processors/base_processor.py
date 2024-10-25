import matplotlib.pyplot as plt
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from config import settings

logger = logging.getLogger(__name__)

class BaseImageProcessor(ABC):
    """Base class for all image processors."""
    def __init__(self):
        self.settings = settings.IMAGE_SETTINGS

    @abstractmethod
    def generate_image(self, data_path: Path, region: str, dataset: str, timestamp: str) -> Path:
        """Each processor must implement this."""
        pass

    def generate_image_path(self, region: str, dataset: str, timestamp: str) -> Path:
        """Generate standardized path for image storage."""
        return settings.REGIONS_DIR / region / "datasets" / dataset / "dates" / timestamp / "image.png"

    def save_image(self, fig, region: str, dataset: str, timestamp: str) -> Path:
        """Save figure to standardized location."""
        try:
            # Add debug logging
            logger.info(f"Figure type: {type(fig)}")
            
            image_path = self.generate_image_path(region, dataset, timestamp)
            image_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Add more specific error handling
            if not hasattr(fig, 'savefig'):
                raise ValueError(f"Expected matplotlib figure, got {type(fig)}")
                
            fig.savefig(image_path, dpi=self.settings['dpi'], bbox_inches='tight')
            plt.close(fig)
            return image_path
        except Exception as e:
            logger.error(f"Error in save_image: {str(e)}")
            raise
