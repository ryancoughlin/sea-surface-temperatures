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
from utils.processing_scheduler import ProcessingScheduler, ProcessingTask
from utils.dates import DateFormatter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def process_historical_data(
    processing_manager: ProcessingManager,
    start_date: datetime,
    days: int = 3
):
    scheduler = ProcessingScheduler(max_concurrent=3)  # Adjust based on resources
    
    # Build all tasks first
    for day_offset in range(days):
        date = start_date - timedelta(days=day_offset)
        for region_id in REGIONS:
            for dataset in SOURCES:
                scheduler.add_task(ProcessingTask(
                    region_id=region_id,
                    dataset=dataset,
                    date=date
                ))
    
    # Process in parallel with controlled concurrency
    stats = await scheduler.run(processing_manager)
    logger.info(f"Processing completed: {stats['successful']} successful, {stats['failed']} failed")

async def main():
    path_manager = PathManager()
    processing_manager = ProcessingManager(path_manager)
    
    async with aiohttp.ClientSession() as session:
        await processing_manager.initialize(session)
        current_date = DateFormatter.get_current_date()  # Get properly formatted UTC date
        await process_historical_data(processing_manager, current_date)

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    asyncio.run(main())