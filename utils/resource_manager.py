from __future__ import annotations  # Enable future annotations

import asyncio
import logging
import psutil
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncGenerator, Dict, Optional
import aiohttp
from contextlib import asynccontextmanager

# Configure module-level logging
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class ResourceLimits:
    """Immutable configuration for resource limits"""
    max_memory_mb: int
    max_concurrent_tasks: int
    connection_limit: int
    connection_limit_per_host: int
    timeout_seconds: int
    min_workers: int = 1
    max_workers: int = 5

    @classmethod
    def default(cls) -> ResourceLimits:
        """Provide sensible defaults"""
        return cls(
            max_memory_mb=1024,
            max_concurrent_tasks=2,
            connection_limit=3,
            connection_limit_per_host=2,
            timeout_seconds=30,
            min_workers=1,
            max_workers=5
        )

class ResourceManager:
    """
    Manages system resources and cleanup for the application.
    
    Responsibilities:
    - Monitor memory usage
    - Manage HTTP sessions
    - Control concurrent operations
    - Handle cleanup
    """
    
    def __init__(
        self,
        limits: Optional[ResourceLimits] = None,
        data_dir: Optional[Path] = None
    ):
        self.limits = limits or ResourceLimits.default()
        self.data_dir = data_dir or Path("./data")
        self._task_semaphore = asyncio.Semaphore(self.limits.max_concurrent_tasks)
        self._active_workers = 0
        self._worker_lock = asyncio.Lock()
        
    def get_optimal_workers(self) -> int:
        """Calculate optimal number of workers based on system resources"""
        cpu_count = psutil.cpu_count(logical=False) or 1
        memory_usage = psutil.virtual_memory().percent
        
        # Reduce workers if memory usage is high
        if memory_usage > 80:
            optimal = self.limits.min_workers
        else:
            optimal = min(cpu_count, self.limits.max_workers)
            
        return max(optimal, self.limits.min_workers)
    
    @asynccontextmanager
    async def managed_session(self) -> AsyncGenerator[aiohttp.ClientSession, None]:
        """
        Creates and manages an aiohttp session with proper configuration and cleanup.
        
        Yields:
            aiohttp.ClientSession: Configured session for HTTP requests
        """
        connector = aiohttp.TCPConnector(
            limit=self.limits.connection_limit,
            limit_per_host=self.limits.connection_limit_per_host,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(total=self.limits.timeout_seconds)
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            raise_for_status=True
        ) as session:
            try:
                yield session
            finally:
                await connector.close()
    
    @asynccontextmanager
    async def managed_worker(self) -> AsyncGenerator[None, None]:
        """Manage worker lifecycle and resources"""
        async with self._worker_lock:
            if self._active_workers >= self.limits.max_workers:
                raise RuntimeError("Maximum worker limit reached")
            self._active_workers += 1
        
        try:
            yield
        finally:
            async with self._worker_lock:
                self._active_workers -= 1
                await self.cleanup_resources()
    
    async def cleanup_resources(self):
        """Cleanup resources after task completion"""
        if not self.check_memory_usage():
            import gc
            gc.collect()
            await asyncio.sleep(0.1)  # Allow event loop to process other tasks
    
    def check_memory_usage(self) -> bool:
        """
        Check if current memory usage is within limits.
        
        Returns:
            bool: True if memory usage is acceptable, False otherwise
        """
        current_memory = psutil.Process().memory_info().rss / (1024 * 1024)  # MB
        is_within_limits = current_memory < self.limits.max_memory_mb
        
        if not is_within_limits:
            logger.warning(
                f"Memory usage ({current_memory:.1f}MB) exceeds limit "
                f"({self.limits.max_memory_mb}MB)"
            )
            
        return is_within_limits
    
    async def cleanup_old_data(self, keep_days: int = 5) -> None:
        """
        Clean up old data files to free space.
        
        Args:
            keep_days: Number of days of data to retain
        """
        try:
            import time
            current_time = time.time()
            
            for path in self.data_dir.glob("**/*"):
                if not path.is_file():
                    continue
                    
                file_age_days = (current_time - path.stat().st_mtime) / (24 * 3600)
                
                if file_age_days > keep_days:
                    logger.info(f"Removing old file: {path}")
                    path.unlink()
                    
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            raise 