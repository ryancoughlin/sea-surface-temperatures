import sys
import os
# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
import aiohttp
from processors.processing_manager import ProcessingManager
from processors.metadata_assembler import MetadataAssembler
from config import settings
from config.settings import SOURCES
from config.regions import REGIONS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def seed_historical_data(days: int = 4):
    """Seed database with historical data for specified number of days"""
    connector = aiohttp.TCPConnector(limit=5)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # Initialize services
        metadata_assembler = MetadataAssembler()
        processing_manager = ProcessingManager(metadata_assembler)
        processing_manager.start_session(session)
        
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
                            
                        # Add small delay to avoid overwhelming ERDDAP server
                        await asyncio.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"Error processing {dataset} for {region_id} on {date.strftime('%Y-%m-%d')}: {str(e)}")

if __name__ == "__main__":
    # Create directories
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    settings.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Run seeder
    asyncio.run(seed_historical_data()) 