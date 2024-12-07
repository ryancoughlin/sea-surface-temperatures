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
    format='%(asctime)s | %(levelname)s | 🔄 %(message)s',
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
        logger.info(f"🚀 Processing {dataset} for region {region_id}")
        
        try:
            # Skip GeoJSON only for united_states region
            skip_geojson = (region_id == "united_states")
            
            result = await self.processing_manager.process_dataset(
                date=date,
                region_id=region_id,
                dataset=dataset,
                skip_geojson=skip_geojson
            )
            
            status = "✅" if result['status'] == 'success' else "❌"
            logger.info(f"{status} {dataset} completed")
            
            return result
        except Exception as e:
            logger.error(f"💥 Failed {dataset}: {str(e)}")
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
            await self.processing_manager.initialize(session)
            
            # Process each region and dataset combination
            for region_id in REGIONS:
                logger.info(f"\n📍 Processing region: {REGIONS[region_id]['name']}")
                for dataset in SOURCES:
                    result = await self.process_dataset(
                        session, date, region_id, dataset
                    )
                    results.append(result)

        # Compile statistics
        successful = sum(1 for r in results if r['status'] == 'success')
        failed = len(results) - successful
        
        logger.info("\n📊 Processing Summary:")
        logger.info(f"   ✅ Successful: {successful}")
        logger.info(f"   ❌ Failed: {failed}")
        logger.info(f"   📝 Total: {len(results)}")
        
        # Group results by region
        for region_id in REGIONS:
            region_results = [r for r in results if r.get('region') == region_id]
            region_success = sum(1 for r in region_results if r['status'] == 'success')
            logger.info(f"\n   {REGIONS[region_id]['name']}:")
            logger.info(f"      ✅ Success: {region_success}")
            logger.info(f"      ❌ Failed: {len(region_results) - region_success}")
        
        # Log details for failed datasets
        if failed > 0:
            logger.info("\nFailed datasets:")
            for result in results:
                if result['status'] == 'error':
                    logger.error(f"   ❌ {result.get('region', 'Unknown')}/{result['dataset']}: {result.get('error', 'Unknown error')}")
        
        return {
            'successful': successful,
            'failed': failed,
            'total': len(results)
        }

async def main():
    """Entry point for data processing"""
    try:
        logger.info("🌊 Starting Oceanographic Data Processing")
        processor = DataProcessor()
        stats = await processor.run()
        
        if stats['failed'] > 0:
            logger.warning(f"⚠️  Completed with {stats['failed']} failures")
        else:
            logger.info("✨ All processing completed successfully")
        
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
