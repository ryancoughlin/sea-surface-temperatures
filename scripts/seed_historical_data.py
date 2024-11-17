import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List
import gc
import aiohttp
from processors.processing_manager import ProcessingManager
from processors.metadata_assembler import MetadataAssembler
from utils.path_manager import PathManager
from config.settings import SOURCES
from config.regions import REGIONS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def process_historical_data(
    processing_manager: ProcessingManager,
    start_date: datetime,
    days: int = 3
):
    """Process historical data with memory management"""
    total_tasks = days * len(REGIONS) * len(SOURCES)
    completed = 0
    
    logger.info(f"Processing {days} days of data")
    logger.info(f"Total tasks: {total_tasks} ({days} days * {len(REGIONS)} regions * {len(SOURCES)} sources)")
    
    # Process one day at a time
    for day_offset in range(days):
        date = start_date - timedelta(days=day_offset)
        logger.info(f"Processing data for {date.strftime('%Y-%m-%d')}")
        
        # Process one region at a time
        for region_id in REGIONS:
            # Process one dataset at a time
            for dataset in SOURCES:
                try:
                    await processing_manager.process_dataset(
                        date=date,
                        region_id=region_id,
                        dataset=dataset
                    )
                    completed += 1
                    logger.info(f"Progress: {completed}/{total_tasks} ({(completed/total_tasks)*100:.1f}%)")
                    logger.info(f"Successfully processed {dataset} for {region_id} on {date.strftime('%Y-%m-%d')}")
                except Exception as e:
                    logger.error(f"Failed to process {dataset} for {region_id} on {date}: {str(e)}")
                
                # Force garbage collection after each dataset
                gc.collect()
                
                # Small delay to allow system to recover
                await asyncio.sleep(1)

async def main():
    path_manager = PathManager()
    metadata_assembler = MetadataAssembler(path_manager)
    processing_manager = ProcessingManager(path_manager, metadata_assembler)
    
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=2, limit_per_host=1),  # Reduced connections
        timeout=aiohttp.ClientTimeout(total=30)
    ) as session:
        await processing_manager.initialize(session)
        await process_historical_data(processing_manager, datetime.now())

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    asyncio.run(main())