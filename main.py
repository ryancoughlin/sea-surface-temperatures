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
import psutil

# Configure logging with cleaner formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)-8s %(message)s',
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
        self.total_tasks = 0
        self.completed_tasks = 0
    
    def add_task(self, task: ProcessingTask):
        self.tasks.append(task)
    
    async def run(self, processing_manager: ProcessingManager) -> dict:
        self.total_tasks = len(self.tasks)
        
        async def process_task(task: ProcessingTask) -> dict:
            task_id = f"{task.dataset}:{task.region_id}"
            try:
                logger.info(f"🚀 Starting task {self.completed_tasks + 1}/{self.total_tasks}: {task_id}")
                async with self.semaphore:
                    result = await processing_manager.process_dataset(
                        date=task.date,
                        region_id=task.region_id,
                        dataset=task.dataset
                    )
                    self.completed_tasks += 1
                    logger.info(f"✅ Completed task {self.completed_tasks}/{self.total_tasks}: {task_id}")
                    return result
            except Exception as e:
                logger.error(f"❌ Task failed ({task_id}): {str(e)}")
                self.completed_tasks += 1
                return {
                    'status': 'error',
                    'error': str(e),
                    'region': task.region_id,
                    'dataset': task.dataset
                }
        
        logger.info(f"Starting processing of {self.total_tasks} tasks with {self.semaphore._value} concurrent workers")
        
        results = await asyncio.gather(
            *[process_task(task) for task in self.tasks],
            return_exceptions=False
        )
        
        return {
            'successful': sum(1 for r in results if r.get('status') == 'success'),
            'failed': sum(1 for r in results if r.get('status') != 'success'),
            'total': self.total_tasks
        }

async def main():
    # Get system info
    mem = psutil.virtual_memory()
    cpu_count = psutil.cpu_count()
    
    # Calculate safe concurrent limits
    safe_concurrent = min(cpu_count, mem.available // (500 * 1024 * 1024))  # 500MB per task
    
    logger.info(f"""
System Resources:
├── Memory: {mem.total / (1024**3):.1f}GB total, {mem.available / (1024**3):.1f}GB available
├── CPUs: {cpu_count}
└── Safe concurrent tasks: {safe_concurrent}
    """.strip())
    
    connector = aiohttp.TCPConnector(
        limit=safe_concurrent + 1,
        limit_per_host=2,
        enable_cleanup_closed=True,
        force_close=True,
    )
    
    timeout = aiohttp.ClientTimeout(
        total=180,        # 3 minutes
        connect=30,
        sock_read=30
    )
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        raise_for_status=True,  # Automatically raise for bad HTTP status codes
        trust_env=True          # Respect HTTP_PROXY and HTTPS_PROXY env variables
    ) as session:
        try:
            path_manager = PathManager()
            metadata_assembler = MetadataAssembler(path_manager)
            processing_manager = ProcessingManager(path_manager, metadata_assembler)
            
            await processing_manager.initialize(session)
            scheduler = ProcessingScheduler(max_concurrent=safe_concurrent)
            
            # Add tasks
            for region_id in REGIONS:
                for dataset in SOURCES:
                    scheduler.add_task(ProcessingTask(
                        region_id=region_id,
                        dataset=dataset,
                        date=datetime.now()
                    ))
            
            stats = await scheduler.run(processing_manager)
            logger.info(f"Processing completed: {stats['successful']} successful, {stats['failed']} failed")
            
        except aiohttp.ClientError as e:
            logger.error(f"Network error occurred: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Processing error: {str(e)}")
            raise
        finally:
            await connector.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise
