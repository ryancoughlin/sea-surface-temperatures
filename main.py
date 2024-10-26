import asyncio
import logging
from datetime import datetime
from pathlib import Path
from config import settings
from config.settings import DATA_DIR, SOURCES
from config.regions import REGIONS
from services.erddap_service import ERDDAPService
from services.service_factory import ServiceFactory
from processors.tile_generator import TileGenerator
from processors.metadata_assembler import MetadataAssembler
from processors.processor_factory import ProcessorFactory
from processors.geojson.factory import GeoJSONConverterFactory

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
        logger.info(f"Processing {region_id} for dataset {dataset}")
        
        region = REGIONS[region_id]
        dataset_config = SOURCES[dataset]
        timestamp = date.strftime('%Y%m%d')

        # Download data with retry logic built into ERDDAPService
        data_file = await service.save_data(
            date=date,
            dataset=dataset_config,
            region=region,
            output_path=DATA_DIR
        )
        
        if not data_file or not data_file.exists():
            raise FileNotFoundError(f"Data file not found for {region_id}, {dataset}")

        # Process data
        processor = ProcessorFactory.create(dataset)
        geojson_converter = GeoJSONConverterFactory.create(dataset)
        
        image_path = processor.generate_image(data_file, region_id, dataset, timestamp)
        geojson_path = geojson_converter.convert(data_file, region_id, dataset, timestamp)

        # Generate metadata
        metadata_path = metadata_assembler.assemble_metadata(
            region=region_id,
            dataset=dataset,
            timestamp=timestamp,
            image_path=image_path,
            geojson_path=geojson_path
        )
        
        return {
            "metadata_path": str(metadata_path),
            "status": "success",
            "data_file": str(data_file),
            "image_path": str(image_path),
            "geojson_path": str(geojson_path)
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
    date = datetime.now()
    tasks = []
    
    # Initialize services and processors
    erddap_service = ERDDAPService()
    tile_generator = TileGenerator()
    metadata_assembler = MetadataAssembler()
    
    # Create tasks for each dataset and region
    for dataset, dataset_config in SOURCES.items():
        if dataset_config['source_type'] == 'erddap':
            for region_id in REGIONS:
                logger.info(f"Creating task: {region_id}, {dataset}")
                task = asyncio.create_task(
                    process_data_for_region_and_dataset(
                        date=date,
                        region_id=region_id,
                        dataset=dataset,
                        service=erddap_service,
                        tile_generator=tile_generator,
                        metadata_assembler=metadata_assembler,
                    )
                )
                tasks.append(task)
    
    # Execute all tasks
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process results
    successes = [r for r in results if isinstance(r, dict) and r.get('status') == 'success']
    failures = [r for r in results if isinstance(r, dict) and r.get('status') == 'error']
    
    logger.info(f"Completed processing: {len(successes)} successful, {len(failures)} failed")
    if failures:
        logger.error("Failed tasks:")
        for failure in failures:
            logger.error(f"  {failure.get('region')}, {failure.get('dataset')}: {failure.get('error')}")

if __name__ == "__main__":
    # Create directories
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    settings.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Run application
    asyncio.run(main())
