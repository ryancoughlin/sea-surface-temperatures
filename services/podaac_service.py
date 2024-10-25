import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict
from .base_service import BaseService
from config.settings import SOURCES

logger = logging.getLogger(__name__)

class PODAACService(BaseService):
    async def download(self, date: datetime, dataset: Dict, region: Dict, output_path: Path) -> Path:
        """Download data using podaac-data-subscriber."""
        try:
            # Find source config (keeping existing code)
            source_config = None
            for key, config in SOURCES.items():
                if config.get('dataset_id') == dataset.get('dataset_id'):
                    source_config = config
                    break
            
            if not source_config:
                raise ValueError(
                    f"No source found for dataset_id '{dataset.get('dataset_id')}' in SOURCES configuration. "
                    f"Available dataset_ids: {[s['dataset_id'] for s in SOURCES.values() if 'dataset_id' in s]}"
                )
            
            # Setup download parameters
            adjusted_date = date - timedelta(days=source_config['lag_days'])
            start_date = adjusted_date.strftime('%Y-%m-%dT00:00:00Z')
            end_date = adjusted_date.strftime('%Y-%m-%dT23:59:59Z')
            
            output_dir = output_path / region['name'].lower().replace(" ", "_") / source_config['name'].lower().replace(" ", "_")
            output_dir.mkdir(parents=True, exist_ok=True)
            bounds = f"{region['bounds'][0][0]},{region['bounds'][0][1]},{region['bounds'][1][0]},{region['bounds'][1][1]}"
            
            cmd_str = (
                f'podaac-data-subscriber '
                f'-c {source_config["collection_shortname"]} '
                f'-d {str(output_dir)} '
                f'-sd {start_date} '
                f'-ed {end_date} '
                f'-b="{bounds}" '
                f'-p {source_config["provider"]} '
                f'-e .nc '
                f'--verbose'
            )
            
            logger.info(f"Executing PODAAC download for {source_config['name']}")
            logger.info("=" * 50)
            logger.info("PODAAC Command:")
            logger.info(cmd_str)
            logger.info("=" * 50)
            
            # Create and run process
            process = await asyncio.create_subprocess_shell(
                cmd_str,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=300)
                
                # Always log output for debugging
                stdout_text = stdout.decode() if stdout else ""
                stderr_text = stderr.decode() if stderr else ""
                
                if stdout_text:
                    logger.debug(f"STDOUT: {stdout_text}")
                if stderr_text:
                    logger.debug(f"STDERR: {stderr_text}")
                
                if process.returncode != 0:
                    raise RuntimeError(f"PODAAC fetch failed with return code {process.returncode}: {stderr_text}")
                
                # Look for downloaded file
                files = list(output_dir.glob(f"*{adjusted_date.strftime('%Y%m%d')}*.nc"))
                if not files:
                    raise FileNotFoundError(f"No matching files found in {output_dir}")
                
                logger.info(f"Successfully downloaded file: {files[0]}")
                return files[0]
                
            except asyncio.TimeoutError:
                logger.error("PODAAC download timed out")
                process.kill()
                raise TimeoutError("PODAAC download timed out after 5 minutes")
                
            except Exception as e:
                logger.error(f"Error during PODAAC download: {str(e)}")
                raise

        except Exception as e:
            logger.error(f"Error in PODAAC download for region {region.get('name', 'unknown')}: {str(e)}")
            raise

    async def download_all_regions(self, date, dataset, regions, output_path):
        """Download data for all regions concurrently with proper error handling."""
        async def download_with_retries(region):
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    return await self.download(date, dataset, region, output_path)
                except Exception as e:
                    if attempt == max_retries - 1:  # Last attempt
                        logger.error(f"Failed all retries for region {region.get('name')}: {str(e)}")
                        raise
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Attempt {attempt + 1} failed for region {region.get('name')}, retrying in {wait_time}s")
                    await asyncio.sleep(wait_time)
        
        # Create tasks for all regions
        tasks = [download_with_retries(region) for region in regions]
        
        # Gather results, allowing individual failures
        results = []
        errors = []
        
        # Wait for all tasks to complete
        done, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        
        for task in done:
            try:
                if not task.exception():
                    results.append(task.result())
            except Exception as e:
                errors.append(e)
                continue
        
        if errors:
            logger.error(f"Completed with {len(errors)} errors out of {len(tasks)} tasks")
        
        return results
