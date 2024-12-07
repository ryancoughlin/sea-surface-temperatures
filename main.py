import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import aiohttp

from config.settings import SOURCES
from config.regions import REGIONS
from processors.metadata_assembler import MetadataAssembler
from processors.processing_manager import ProcessingManager
from processors.processing_config import ProcessingConfig
from processors.processing_result import ProcessingResult
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
                            config: ProcessingConfig) -> ProcessingResult:
        """Process single dataset for a region"""
        logger.info(f"🚀 Processing {config.dataset} for region {config.region_id}")
        
        try:
            result = await self.processing_manager.process_dataset(
                date=config.date,
                region_id=config.region_id,
                dataset=config.dataset,
                skip_geojson=config.should_skip_geojson()
            )
            
            status = "✅" if result['status'] == 'success' else "❌"
            logger.info(f"{status} {config.dataset} completed")
            
            return ProcessingResult(**result)
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"💥 Failed {config.dataset}: {error_msg}")
            return ProcessingResult.error(
                dataset=config.dataset,
                region=config.region_id,
                error=error_msg
            )

    def _group_results_by_region(self, results: List[ProcessingResult]) -> Dict[str, List[ProcessingResult]]:
        """Group processing results by region"""
        grouped = {}
        for result in results:
            if result.region not in grouped:
                grouped[result.region] = []
            grouped[result.region].append(result)
        return grouped

    def _log_processing_summary(self, results: List[ProcessingResult], grouped_results: Dict[str, List[ProcessingResult]]):
        """Log processing summary statistics"""
        successful = sum(1 for r in results if r.is_success)
        failed = len(results) - successful
        
        logger.info("\n📊 Processing Summary:")
        logger.info(f"   ✅ Successful: {successful}")
        logger.info(f"   ❌ Failed: {failed}")
        logger.info(f"   📝 Total: {len(results)}")
        
        # Log region-specific stats
        for region_id, region_results in grouped_results.items():
            region_success = sum(1 for r in region_results if r.is_success)
            logger.info(f"\n   {REGIONS[region_id]['name']}:")
            logger.info(f"      ✅ Success: {region_success}")
            logger.info(f"      ❌ Failed: {len(region_results) - region_success}")
        
        # Log failed datasets
        if failed > 0:
            logger.info("\nFailed datasets:")
            for result in results:
                if not result.is_success:
                    logger.error(f"   ❌ {result.region}/{result.dataset}: {result.error}")

    async def run(self) -> Dict[str, int]:
        """Process all datasets for all regions"""
        date = datetime.now()
        results: List[ProcessingResult] = []
        
        async with aiohttp.ClientSession() as session:
            await self.processing_manager.initialize(session)
            
            # Process each region and dataset combination
            for region_id in REGIONS:
                logger.info(f"\n📍 Processing region: {REGIONS[region_id]['name']}")
                for dataset in SOURCES:
                    config = ProcessingConfig(
                        date=date,
                        region_id=region_id,
                        dataset=dataset
                    )
                    result = await self.process_dataset(session, config)
                    results.append(result)

        # Log processing summary
        grouped_results = self._group_results_by_region(results)
        self._log_processing_summary(results, grouped_results)
        
        successful = sum(1 for r in results if r.is_success)
        return {
            'successful': successful,
            'failed': len(results) - successful,
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
