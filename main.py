import asyncio
import logging
import aiohttp
from datetime import datetime
from pathlib import Path
from config.settings import SOURCES
from config.regions import REGIONS
from processors.metadata_assembler import MetadataAssembler
from processors.processing_manager import ProcessingManager
from utils.path_manager import PathManager
from typing import List, Dict
from dataclasses import dataclass
from functools import partial

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ProcessingTask:
    region_id: str
    dataset: str
    date: datetime
    priority: int = 1

class ProcessingScheduler:
    def __init__(self, max_concurrent: int = 2):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.tasks: List[ProcessingTask] = []
        
    def add_task(self, task: ProcessingTask):
        self.tasks.append(task)
        
    async def process_task(self, processing_manager, task: ProcessingTask) -> Dict:
        async with self.semaphore:
            try:
                logger.info(f"Processing {task.dataset} for {task.region_id}")
                result = await processing_manager.process_dataset(
                    date=task.date,
                    region_id=task.region_id,
                    dataset=task.dataset
                )
                return {
                    'task': task,
                    'status': result['status'],
                    'error': None
                }
            except Exception as e:
                logger.error(f"Error processing {task.dataset} for {task.region_id}: {str(e)}")
                return {
                    'task': task,
                    'status': 'failed',
                    'error': str(e)
                }

    async def run(self, processing_manager) -> Dict[str, int]:
        # Sort tasks by priority
        self.tasks.sort(key=lambda x: x.priority, reverse=True)
        
        # Process in controlled batches
        results = await asyncio.gather(*[
            self.process_task(processing_manager, task) 
            for task in self.tasks
        ])
        
        # Count results
        stats = {'successful': 0, 'failed': 0}
        for result in results:
            if result['status'] == 'success':
                stats['successful'] += 1
            else:
                stats['failed'] += 1
                
        return stats

async def main():
    # Initialize core services
    path_manager = PathManager()
    path_manager.cleanup_old_data(keep_days=5)
    
    metadata_assembler = MetadataAssembler(path_manager)
    processing_manager = ProcessingManager(path_manager, metadata_assembler)
    
    # Get current date
    date = datetime.now()
    
    # Configure aiohttp session with connection pooling
    connector = aiohttp.TCPConnector(
        limit=3,
        limit_per_host=2,
        enable_cleanup_closed=True
    )
    
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        raise_for_status=True
    ) as session:
        # Initialize processing manager with session
        await processing_manager.initialize(session)
        
        # Initialize scheduler with controlled concurrency
        scheduler = ProcessingScheduler(max_concurrent=2)
        
        # Add tasks with priorities
        for region_id in REGIONS:
            for dataset in SOURCES:
                priority = SOURCES[dataset].get('priority', 1)
                scheduler.add_task(ProcessingTask(
                    region_id=region_id,
                    dataset=dataset,
                    date=date,
                    priority=priority
                ))
        
        # Run processing with controlled concurrency
        results = await scheduler.run(processing_manager)
        
        logger.info(f"Completed: {results['successful']} successful, {results['failed']} failed")

if __name__ == "__main__":
    asyncio.run(main())
