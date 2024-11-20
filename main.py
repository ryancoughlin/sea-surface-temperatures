import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict
import aiohttp

from config.settings import SOURCES
from config.regions import REGIONS
from processors.metadata_assembler import MetadataAssembler
from processors.processing_manager import ProcessingManager
from utils.path_manager import PathManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class DataProcessor:
    """Coordinates oceanographic data processing"""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.path_manager = PathManager()
        self.metadata_assembler = MetadataAssembler(self.path_manager)
        self.processing_manager = ProcessingManager(
            self.path_manager,
            self.metadata_assembler
        )
        
    async def process_dataset(self, session: aiohttp.ClientSession, 
                            date: datetime, region_id: str, dataset: str) -> Dict:
        """Process single dataset for a region"""
        logger.info(f"Processing dataset={dataset} region={region_id}")
        
        try:
            result = await self.processing_manager.process_dataset(
                date=date,
                region_id=region_id,
                dataset=dataset
            )
            return {
                'status': 'success',
                'dataset': dataset,
                'region': region_id,
                'result': result
            }
        except Exception as e:
            logger.error(f"Failed {dataset}/{region_id}: {str(e)}")
            return {
                'status': 'error',
                'dataset': dataset,
                'region': region_id,
                'error': str(e)
            }

    async def run(self) -> Dict[str, int]:
        """Process all datasets for all regions"""
        date = datetime.now()

        results = []
        
        async with aiohttp.ClientSession() as session:
            # Initialize services
            await self.processing_manager.initialize(session)
            
            # Process each dataset/region combination
            for region_id in REGIONS:
                for dataset in SOURCES:
                    result = await self.process_dataset(
                        session, date, region_id, dataset
                    )
                    results.append(result)

        successful = sum(1 for r in results if r['status'] == 'success')
        failed = len(results) - successful
        
        return {
            'successful': successful,
            'failed': failed,
            'total': len(results)
        }

async def main():
    """Entry point for data processing"""
    try:
        processor = DataProcessor()
        stats = await processor.run()
        
        logger.info("Processing Summary:")
        logger.info(f"Successful: {stats['successful']}")
        logger.info(f"Failed: {stats['failed']}")
        logger.info(f"Total: {stats['total']}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
