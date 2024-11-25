import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
import aiohttp

from processors.metadata_assembler import MetadataAssembler
from processors.processing_manager import ProcessingManager
from utils.path_manager import PathManager
from config.settings import SOURCES
from config.regions import REGIONS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def cleanup_metadata():
    """Remove existing metadata.json file"""
    metadata_path = Path("output") / "metadata.json"
    if metadata_path.exists():
        logger.info("Removing existing metadata.json")
        metadata_path.unlink()
        logger.info("✅ Metadata file removed")

class HistoricalDataProcessor:
    def __init__(self):
        self.path_manager = PathManager()
        self.metadata_assembler = MetadataAssembler(self.path_manager)
        self.processing_manager = ProcessingManager(
            self.path_manager,
            self.metadata_assembler
        )

    async def process_date(self, session: aiohttp.ClientSession, date: datetime) -> dict:
        """Process all datasets and regions for a specific date"""
        await self.processing_manager.initialize(session)
        
        results = []
        for region_id in REGIONS:
            for dataset in SOURCES:
                logger.info(f"Processing {dataset} for {region_id} on {date.strftime('%Y-%m-%d')}")
                try:
                    result = await self.processing_manager.process_dataset(
                        date=date,
                        region_id=region_id,
                        dataset=dataset
                    )
                    results.append(result)
                except Exception as e:
                    logger.error(f"Failed to process {dataset}/{region_id}: {str(e)}")
                    results.append({
                        'status': 'error',
                        'dataset': dataset,
                        'region': region_id,
                        'error': str(e)
                    })

        successful = sum(1 for r in results if r['status'] == 'success')
        return {
            'date': date.strftime('%Y-%m-%d'),
            'successful': successful,
            'failed': len(results) - successful,
            'total': len(results)
        }

async def process_historical_data(days: int = 5):
    """Process historical data for specified number of days"""
    cleanup_metadata()
    
    processor = HistoricalDataProcessor()
    
    # Set the end date to the current time in UTC
    end_date = datetime.now(timezone.utc)

    # Generate the list of dates
    dates = [end_date - timedelta(days=i) for i in range(days)]
    
    total_results = []
    async with aiohttp.ClientSession() as session:
        for date in dates:
            logger.info(f"\n=== Processing data for {date.strftime('%Y-%m-%d')} ===")
            result = await processor.process_date(session, date)
            total_results.append(result)
            
            logger.info(f"Daily Summary for {result['date']}:")
            logger.info(f"✅ Successful: {result['successful']}")
            logger.info(f"❌ Failed: {result['failed']}")
            logger.info(f"📊 Total: {result['total']}")
    
    total_successful = sum(r['successful'] for r in total_results)
    total_failed = sum(r['failed'] for r in total_results)
    total_processed = sum(r['total'] for r in total_results)
    
    logger.info("\n=== Final Processing Summary ===")
    logger.info(f"Days Processed: {len(dates)}")
    logger.info(f"Total Successful: {total_successful}")
    logger.info(f"Total Failed: {total_failed}")
    logger.info(f"Total Processed: {total_processed}")

if __name__ == "__main__":
    asyncio.run(process_historical_data())