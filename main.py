import asyncio
import logging
import xarray as xr
from config import settings
from datetime import datetime
from pathlib import Path
from PIL import Image
from config.settings import DATA_DIR, SOURCES
from config.regions import REGIONS
from services.base_service import BaseService
from services.service_factory import ServiceFactory
from processors.tile_generator import TileGenerator
from processors.metadata_assembler import MetadataAssembler
from processors.processor_factory import ProcessorFactory
from processors.geojson.factory import GeoJSONConverterFactory

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def process_data_for_region_and_dataset(
    date: datetime,
    region_id: str,
    dataset: str,
    service: BaseService,
    tile_generator: TileGenerator,
    metadata_assembler: MetadataAssembler,
) -> dict:
    """Process data for a single region and dataset, and return metadata."""
    try:
        logging.info(f"Starting processing for region: {region_id}, dataset: {dataset}")
        
        # Get configurations
        region = REGIONS[region_id]
        dataset_config = SOURCES[dataset]
        timestamp = date.strftime('%Y%m%d')

        # Download data
        data_file = await service.download(
            date=date,
            dataset=dataset_config,
            region=region,
            output_path=DATA_DIR
        )
        
        # Create processor and converter
        processor = ProcessorFactory.create(dataset)
        geojson_converter = GeoJSONConverterFactory.create(dataset)
        
        # Generate image and GeoJSON
        image_path = processor.generate_image(
            data_path=data_file,
            region=region_id,
            dataset=dataset,
            timestamp=timestamp
        )
        
        geojson_path = geojson_converter.convert(
            data_path=data_file,
            region=region_id,
            dataset=dataset,
            timestamp=timestamp
        )
        
        # Generate metadata
        metadata_path = metadata_assembler.assemble_metadata(
            region=region_id,
            dataset=dataset,
            timestamp=timestamp,
            image_path=image_path,
            geojson_path=geojson_path
        )
        
        # Generate tiles
        tile_generator.generate_tiles(
            Image.open(image_path), 
            region_id, 
            dataset, 
            timestamp
        )
        
        return {"metadata_path": str(metadata_path)}
        
    except Exception as e:
        logging.error(f"Error processing {region_id} for dataset {dataset}: {str(e)}")
        return {}

async def main():
    # Initialize components
    service_factory = ServiceFactory()
    tile_generator = TileGenerator()
    metadata_assembler = MetadataAssembler()
    date = datetime.now()
    tasks = []
    
    for dataset in SOURCES:
        dataset_config = SOURCES[dataset]
        service = service_factory.get_service(dataset_config['source_type'])
        
        for region_id in REGIONS:
            logging.info(f"Creating task for region: {region_id}, dataset: {dataset}")
            task = process_data_for_region_and_dataset(
                date, 
                region_id, 
                dataset, 
                service,
                tile_generator,
                metadata_assembler,
            )
            tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    # Create necessary directories
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    settings.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Run the application
    asyncio.run(main())
