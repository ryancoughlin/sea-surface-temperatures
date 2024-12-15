import asyncio
import logging
from datetime import datetime
import aiohttp

from config.settings import SOURCES
from config.regions import REGIONS
from processors.orchestration.processing_manager import ProcessingManager
from processors.data.data_assembler import DataAssembler
from utils.path_manager import PathManager

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

async def main():
    """Process oceanographic data for all regions and datasets"""
    try:
        logger.info("Starting oceanographic data processing")
        
        # Initialize core dependencies
        path_manager = PathManager()
        data_assembler = DataAssembler(path_manager)
        processing_manager = ProcessingManager(path_manager, data_assembler)
        
        # Process datasets sequentially
        async with aiohttp.ClientSession() as session:
            await processing_manager.initialize(session)
            
            for region_id in REGIONS:
                for dataset in SOURCES:
                    try:
                        logger.info(f"Processing {dataset} for region {region_id}")
                        result = await processing_manager.process_dataset(
                            date=datetime.now(),
                            region_id=region_id,
                            dataset=dataset,
                            skip_geojson=False
                        )
                        
                        if result['status'] == 'success':
                            logger.info(f"Completed {dataset} for {region_id}")
                        else:
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
