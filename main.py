import asyncio
import logging
from datetime import datetime
from pathlib import Path
from config import settings
from config.settings import DATA_DIR, SOURCES
from config.regions import REGIONS
from services.erddap_service import ERDDAPService
from processors.tile_generator import TileGenerator
from processors.metadata_assembler import MetadataAssembler
from processors.processor_factory import ProcessorFactory
from processors.geojson.factory import GeoJSONConverterFactory
from processors.processing_manager import ProcessingManager
import aiohttp

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def process_data_for_region_and_dataset(
    date: datetime,
    region_id: str,
    dataset: str,
    service: ERDDAPService,
    tile_generator: TileGenerator,
    metadata_assembler: MetadataAssembler,
) -> dict:
    """Process data for a single region and dataset"""
    try:
        region = REGIONS[region_id]
        dataset_config = SOURCES[dataset]
        timestamp = date.strftime('%Y%m%d')

        # Download data returns Path
        data_file: Path = await service.save_data(
            date=date,
            dataset=dataset_config,
            region=region,
            output_path=DATA_DIR
        )
        
        if not data_file.exists():
            raise FileNotFoundError(f"Data file not found for {region_id}, {dataset}")

        # Process data - all methods return Path objects
        processor = ProcessorFactory.create(dataset)
        geojson_converter = GeoJSONConverterFactory.create(dataset)
        
        image_path: Path = processor.generate_image(data_file, region_id, dataset, timestamp)
        geojson_path: Path = geojson_converter.convert(data_file, region_id, dataset, timestamp)

        # Generate metadata returns Path
        metadata_path: Path = metadata_assembler.assemble_metadata(
            region=region_id,
            dataset=dataset,
            timestamp=timestamp,
            image_path=image_path,
            geojson_path=geojson_path
        )
        
        return {
            "status": "success",
            "paths": {
                "data_file": data_file,
                "image_path": image_path,
                "geojson_path": geojson_path,
                "metadata_path": metadata_path
            },
            "region": region_id,
            "dataset": dataset
        }
        
    except Exception as e:
        logger.error(f"Error processing {region_id} for {dataset}: {str(e)}")
        return {
            "status": "error",
            "error": str(e),
            "region": region_id,
            "dataset": dataset
        }

async def main():
    # Configure connection pooling
    connector = aiohttp.TCPConnector(limit=5)  # Limit concurrent connections
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # Initialize services
        metadata_assembler = MetadataAssembler()
        processing_manager = ProcessingManager(metadata_assembler)
        processing_manager.start_session(session)
        
        date = datetime.now()
        tasks = []
        
        # Create processing tasks
        for dataset in SOURCES:
            for region_id in REGIONS:
                task = asyncio.create_task(
                    processing_manager.process_dataset(
                        date=date,
                        region_id=region_id,
                        dataset=dataset
                    )
                )
                tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        successes = [r for r in results if isinstance(r, dict) and r.get('status') == 'success']
        failures = [r for r in results if isinstance(r, dict) and r.get('status') == 'error']
        
        logger.info(f"Completed: {len(successes)} successful, {len(failures)} failed")

if __name__ == "__main__":
    # Create directories
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    settings.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Run application
    asyncio.run(main())
