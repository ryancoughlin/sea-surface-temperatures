import asyncio
import logging
import aiohttp
from datetime import datetime
from pathlib import Path
from config.settings import SOURCES
from config.regions import REGIONS
from processors.metadata_assembler import MetadataAssembler
from processors.processing_manager import ProcessingManager
from utils.path_manager import PathManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    # Initialize core services
    path_manager = PathManager()
    metadata_assembler = MetadataAssembler(path_manager)
    processing_manager = ProcessingManager(path_manager, metadata_assembler)
    
    # Get current date
    date = datetime.now()
    
    # Track results
    successful = 0
    failed = 0
    
    async with aiohttp.ClientSession() as session:
        # Initialize processing manager with session
        await processing_manager.initialize(session)
        
        # Process each dataset for each region
        tasks = []
        for region_id in REGIONS:
            for dataset in SOURCES:
                tasks.append(
                    processing_manager.process_dataset(
                        date=date,
                        region_id=region_id,
                        dataset=dataset
                    )
                )
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Count successes and failures
        for result in results:
            if result['status'] == 'success':
                successful += 1
            else:
                failed += 1
    
    logger.info(f"Completed: {successful} successful, {failed} failed")

if __name__ == "__main__":
    asyncio.run(main())
