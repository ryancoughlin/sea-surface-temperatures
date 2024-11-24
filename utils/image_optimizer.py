from PIL import Image
import io
import logging
from pathlib import Path
import numpy as np

logger = logging.getLogger(__name__)

class ImageOptimizer:
    def __init__(self, quality=85, optimize=True):
        self.quality = quality
        self.optimize = optimize

    def optimize_png(self, image_path: Path) -> None:
        """Optimize PNG images while maintaining quality."""
        try:
            # Open image
            with Image.open(image_path) as img:
                # Create buffer
                buffer = io.BytesIO()
                
                # Save with optimization
                img.save(
                    buffer, 
                    format='PNG',
                    optimize=self.optimize,
                    quality=self.quality,
                    # PNG-specific optimizations
                    compress_level=6,  # Balance between size and speed
                    bits=8  # Reduce color depth if possible
                )
                
                # Write back to file
                with open(image_path, 'wb') as f:
                    f.write(buffer.getvalue())
                
                logger.info(f"Optimized image: {image_path}")
                
        except Exception as e:
            logger.error(f"Error optimizing image {image_path}: {e}")
            raise 