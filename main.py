import asyncio
import logging
from datetime import datetime
from dataclasses import dataclass
from config.settings import SOURCES
from config.regions import REGIONS
from processors.metadata_assembler import MetadataAssembler
from processors.processing_manager import ProcessingManager
from utils.path_manager import PathManager
import aiohttp

# Configure logging with better formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class ProcessingTask:
    region_id: str
    dataset: str
    date: datetime

class ProcessingScheduler:
    def __init__(self, max_concurrent: int = 3):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.tasks = []
    
    def add_task(self, task: ProcessingTask):
        self.tasks.append(task)
    
    async def run(self, processing_manager: ProcessingManager) -> dict:
        async def process_task(task: ProcessingTask) -> dict:
            try:
                async with self.semaphore:
                    return await processing_manager.process_dataset(
                        date=task.date,
                        region_id=task.region_id,
                        dataset=task.dataset
                    )
            except Exception as e:
                logger.error(f"Task failed: {str(e)}")
                return {
                    'status': 'error',
                    'error': str(e),
                    'region': task.region_id,
                    'dataset': task.dataset
                }
        
        results = await asyncio.gather(
            *[process_task(task) for task in self.tasks],
            return_exceptions=False
        )
        
        return {
            'successful': sum(1 for r in results if r.get('status') == 'success'),
            'failed': sum(1 for r in results if r.get('status') != 'success')
        }

async def main():
    path_manager = PathManager()
    metadata_assembler = MetadataAssembler(path_manager)
    processing_manager = ProcessingManager(path_manager, metadata_assembler)
    
    connector = aiohttp.TCPConnector(limit=3, limit_per_host=2)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        await processing_manager.initialize(session)
        scheduler = ProcessingScheduler()
        
        # Add tasks
        for region_id in REGIONS:
            for dataset in SOURCES:
                scheduler.add_task(ProcessingTask(
                    region_id=region_id,
                    dataset=dataset,
                    date=datetime.now()
                ))
        
        try:
            stats = await scheduler.run(processing_manager)
            logger.info(f"Processing completed: {stats['successful']} successful, {stats['failed']} failed")
        except Exception as e:
            logger.error(f"Processing error: {str(e)}")
            raise

if __name__ == "__main__":
    asyncio.run(main())
