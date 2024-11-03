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
    
    # Configure aiohttp session
    connector = aiohttp.TCPConnector(
        limit=3,
        limit_per_host=2,
        enable_cleanup_closed=True
    )
    
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        raise_for_status=True
    ) as session:
        # Initialize processing manager with session
        await processing_manager.initialize(session)
        
        # Process datasets sequentially for each region
        for region_id in REGIONS:
            for dataset in SOURCES:
                try:
                    result = await processing_manager.process_dataset(
                        date=date,
                        region_id=region_id,
                        dataset=dataset
                    )
                    
                    if result['status'] == 'success':
                        successful += 1
                    else:
                        failed += 1
                        
                except Exception as e:
                    logger.error(f"Error processing {dataset} for {region_id}: {str(e)}")
                    failed += 1
                    continue
    
    logger.info(f"Completed: {successful} successful, {failed} failed")

if __name__ == "__main__":
    asyncio.run(main())
