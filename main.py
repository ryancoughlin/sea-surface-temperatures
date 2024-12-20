import asyncio
import logging
from datetime import datetime
import aiohttp
from pathlib import Path

from config.settings import SOURCES
from config.regions import REGIONS
from processors.orchestration.processing_manager import ProcessingManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

async def main():
    """Process oceanographic data for all regions and datasets"""
    try:
        logger.info("Starting oceanographic data processing")
        
        base_dir = Path(__file__).parent
        processing_manager = ProcessingManager(base_dir)
        
        async with aiohttp.ClientSession() as session:
            await processing_manager.initialize(session)
            
            for region_id in REGIONS:
                for dataset in SOURCES:
                    try:
                        result = await processing_manager.process_dataset(
                            date=datetime.now(),
                            region_id=region_id,
                            dataset=dataset,
                            skip_geojson=False
                        )
                        
                        if result['status'] != 'success':
                            logger.error(f"Failed {dataset} for {region_id}: {result.get('error', 'Unknown error')}")
                            
                    except Exception as e:
                        logger.error(f"Error processing {dataset} for {region_id}: {str(e)}")
                        continue
        
        logger.info("Processing completed")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
