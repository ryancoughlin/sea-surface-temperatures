from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from services.erddap_service import ERDDAPService
from processors.tile_generator import TileGenerator
from processors.metadata_assembler import MetadataAssembler
from processors.geojson.factory import GeoJSONConverterFactory
from processors.processor_factory import ProcessorFactory
from config.settings import DATA_DIR, SOURCES
from config.regions import REGIONS
import logging

logger = logging.getLogger(__name__)

class ProcessingManager:
    """Coordinates data processing workflow using existing services"""
    
    def __init__(self, metadata_assembler: MetadataAssembler):
        self.metadata_assembler = metadata_assembler
        self.erddap_service = ERDDAPService()
        self.tile_generator = TileGenerator()
        self.processor_factory = ProcessorFactory()
    
    async def process_dataset(
        self, 
        date: datetime,
        region_id: str, 
        dataset: str
    ) -> Dict:
        """Orchestrates the processing workflow"""
        try:
            timestamp = date.strftime('%Y%m%d')
            
            # Download data
            data_file = await self.erddap_service.save_data(
                date=date,
                dataset=SOURCES[dataset],
                region=REGIONS[region_id],
                output_path=DATA_DIR
            )
            
            # Process data
            processor = self.processor_factory.create(dataset)
            image_path = processor.generate_image(
                data_file, 
                region_id, 
                dataset, 
                timestamp
            )
            
            # Generate GeoJSON
            geojson_converter = GeoJSONConverterFactory.create(dataset)
            geojson_path = geojson_converter.convert(
                data_file, 
                region_id, 
                dataset, 
                timestamp
            )
            
            # Generate tiles
            self.tile_generator.generate_tiles(
                image=image_path,
                region=region_id,
                dataset=dataset,
                timestamp=timestamp
            )
            
            # Use existing metadata assembler
            metadata_path = self.metadata_assembler.assemble_metadata(
                region=region_id,
                dataset=dataset,
                timestamp=timestamp,
                image_path=image_path,
                geojson_path=geojson_path,
                mapbox_url=mapbox_url
            )
            
            return {
                "status": "success",
                "metadata_path": str(metadata_path),
                "image_path": str(image_path),
                "geojson_path": str(geojson_path)
            }
            
        except Exception as e:
            logger.error(f"Error processing {dataset} for {region_id}: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "region": region_id,
                "dataset": dataset
            }
