import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import logging
from datetime import datetime, timedelta
import aiohttp
from processors.processing_manager import ProcessingManager
from processors.metadata_assembler import MetadataAssembler
from utils.path_manager import PathManager
from config.settings import SOURCES
from config.regions import REGIONS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def seed_historical_data(days: int = 5):
    """Seed database with historical data for specified number of days"""
    # Initialize managers in correct order
    path_manager = PathManager()
    path_manager.ensure_directories()
    
    # Initialize metadata assembler with path manager
    metadata_assembler = MetadataAssembler(path_manager)
    
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Initialize processing manager and its dependencies
        processing_manager = ProcessingManager(path_manager, metadata_assembler)
        await processing_manager.initialize(session)  # Initialize async services
        
        # Generate dates (most recent first)
        today = datetime.now()
        dates = [today - timedelta(days=x) for x in range(1, days + 1)]
        
        total_tasks = len(dates) * len(SOURCES) * len(REGIONS)
        completed = 0
        
        # Process each date
        for date in dates:
            logger.info(f"Processing data for {date.strftime('%Y-%m-%d')}")
            
            for dataset in SOURCES:
                for region_id in REGIONS:
                    try:
                        # Get paths for this dataset
                        data_path = path_manager.get_data_path(date, dataset, region_id)
                        asset_paths = path_manager.get_asset_paths(date, dataset, region_id)
                        
                        # Process dataset
                        result = await processing_manager.process_dataset(
                            date=date,
                            region_id=region_id,
                            dataset=dataset
                        )
                        
                        completed += 1
                        logger.info(f"Progress: {completed}/{total_tasks} ({(completed/total_tasks)*100:.1f}%)")
                        
                        if result['status'] == 'success':
                            logger.info(f"Successfully processed {dataset} for {region_id} on {date.strftime('%Y-%m-%d')}")
                        else:
                            logger.error(f"Failed to process {dataset} for {region_id} on {date.strftime('%Y-%m-%d')}: {result.get('error')}")
                            
                        await asyncio.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"Error processing {dataset} for {region_id} on {date.strftime('%Y-%m-%d')}: {str(e)}")

if __name__ == "__main__":
    # Run seeder
    asyncio.run(seed_historical_data())