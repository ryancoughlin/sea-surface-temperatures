import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import aiohttp
import time

from config.settings import SOURCES
from config.regions import REGIONS
from processors.data.data_assembler import DataAssembler
from processors.orchestration.processing_orchestrator import ProcessingManager
from processors.processing_config import ProcessingConfig
from processors.processing_result import ProcessingResult
from processors.cleanup_manager import CleanupManager
from utils.path_manager import PathManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | 🔄 %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class DataProcessor:
    """Coordinates oceanographic data processing"""
    
    def __init__(self, output_dir: str = "output", max_concurrent_tasks: int = 3):
        self.output_dir = Path(output_dir)
        self.path_manager = PathManager()
        self.metadata_assembler = DataAssembler(self.path_manager)
        self.processing_manager = ProcessingManager(
            self.path_manager,
            self.metadata_assembler
        )
        self.cleanup_manager = CleanupManager(self.path_manager)
        self.max_concurrent_tasks = max_concurrent_tasks
        
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

    async def process_batch(self, session: aiohttp.ClientSession, configs: List[ProcessingConfig]) -> List[ProcessingResult]:
        """Process a batch of datasets concurrently"""
        tasks = [self.process_dataset(session, config) for config in configs]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def run(self) -> Dict[str, int]:
        """Process all datasets for all regions with controlled concurrency"""
        start_time = time.time()
        date = datetime.now()
        results: List[ProcessingResult] = []
        
        # Run cleanup before processing
        logger.info("🧹 Running data cleanup")
        self.cleanup_manager.cleanup_all()
        
        # Create all processing configs
        configs = [
            ProcessingConfig(
                date=date,
                region_id=region_id,
                dataset=dataset
            )
            for region_id in REGIONS
            for dataset in SOURCES
        ]
        
        # Process in batches with controlled concurrency
        async with aiohttp.ClientSession() as session:
            await self.processing_manager.initialize(session)
            
            for i in range(0, len(configs), self.max_concurrent_tasks):
                batch = configs[i:i + self.max_concurrent_tasks]
                batch_results = await self.process_batch(session, batch)
                results.extend(batch_results)
                
                # Small delay between batches to prevent resource exhaustion
                await asyncio.sleep(0.5)
        
        # Calculate total processing time
        total_time = time.time() - start_time
        hours = int(total_time // 3600)
        minutes = int((total_time % 3600) // 60)
        seconds = int(total_time % 60)
        
        # Log processing summary
        grouped_results = self._group_results_by_region(results)
        self._log_processing_summary(results, grouped_results, total_time)
        
        successful = sum(1 for r in results if r.is_success)
        return {
            'successful': successful,
            'failed': len(results) - successful,
            'total': len(results),
            'processing_time': total_time
        }

    def _group_results_by_region(self, results: List[ProcessingResult]) -> Dict[str, List[ProcessingResult]]:
        """Group processing results by region"""
        grouped = {}
        for result in results:
            if result.region not in grouped:
                grouped[result.region] = []
            grouped[result.region].append(result)
        return grouped

    def _log_processing_summary(self, results: List[ProcessingResult], 
                              grouped_results: Dict[str, List[ProcessingResult]], 
                              total_time: float):
        """Log processing summary statistics"""
        successful = sum(1 for r in results if r.is_success)
        failed = len(results) - successful
        
        # Format time components
        hours = int(total_time // 3600)
        minutes = int((total_time % 3600) // 60)
        seconds = int(total_time % 60)
        
        logger.info("\n📊 Processing Summary:")
        logger.info(f"   ✅ Successful: {successful}")
        logger.info(f"   ❌ Failed: {failed}")
        logger.info(f"   📝 Total: {len(results)}")
        logger.info(f"   ⏱️  Total Processing Time: {hours:02d}:{minutes:02d}:{seconds:02d}")
        
        # Log failed datasets without region summaries
        if failed > 0:
            logger.info("\nFailed datasets:")
            for result in results:
                if not result.is_success:
                    logger.error(f"   ❌ {result.dataset}: {result.error}")

async def main():
    """Entry point for data processing"""
    try:
        logger.info("🌊 Starting Oceanographic Data Processing")
        # Adjust max_concurrent_tasks based on your DigitalOcean box capacity
        processor = DataProcessor(max_concurrent_tasks=4)
        stats = await processor.run()
        
        if stats['failed'] > 0:
            logger.warning(f"⚠️  Completed with {stats['failed']} failures")
        else:
            logger.info("✨ All processing completed successfully")
            
        # Log total processing time
        total_time = stats['processing_time']
        hours = int(total_time // 3600)
        minutes = int((total_time % 3600) // 60)
        seconds = int(total_time % 60)
        logger.info(f"⏱️  Total Runtime: {hours:02d}:{minutes:02d}:{seconds:02d}")
        
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
