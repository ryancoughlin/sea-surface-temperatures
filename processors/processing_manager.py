from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Union
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
        """Initialize with required services"""
        # Core services
        self.metadata_assembler = metadata_assembler
        self.erddap_service = ERDDAPService()
        self.tile_generator = TileGenerator()
        
        # Factories
        self.processor_factory = ProcessorFactory()
        self.geojson_converter_factory = GeoJSONConverterFactory()

    async def process_dataset(
        self, 
        date: datetime,
        region_id: str, 
        dataset: str
    ) -> Dict[str, Union[str, Path, dict]]:
        """Process a single dataset for a region."""
        try:
            region = REGIONS[region_id]
            dataset_config = SOURCES[dataset]
            timestamp = date.strftime('%Y%m%d')

            # Download data using ERDDAPService directly
            data_path: Path = await self.erddap_service.save_data(
                date=date,
                dataset=dataset_config,
                region=region,
                output_path=DATA_DIR
            )
            
            if not data_path.exists():
                raise FileNotFoundError(f"Data file not found for {region_id}, {dataset}")

            # Process data
            processor = ProcessorFactory.create(dataset)
            logger.info(f"Processing {dataset} data for {region_id}")

            # Generate image with all original processing logic
            image_path: Path = processor.generate_image(
                data_path=data_path,
                region=region_id,
                dataset=dataset,
                timestamp=timestamp
            )
            
            # Convert to GeoJSON with original conversion logic
            geojson_converter = GeoJSONConverterFactory.create(dataset)
            geojson_path: Path = geojson_converter.convert(
                data_path=data_path,
                region=region_id,
                dataset=dataset,
                timestamp=timestamp
            )

            # Generate metadata
            metadata_path: Path = self.metadata_assembler.assemble_metadata(
                region=region_id,
                dataset=dataset,
                timestamp=timestamp,
                image_path=image_path,
                geojson_path=geojson_path
            )

            # Validate all outputs exist and have content
            outputs = {
                'data': data_path,
                'image': image_path,
                'geojson': geojson_path,
                'metadata': metadata_path
            }

            if all(path.exists() and path.stat().st_size > 0 for path in outputs.values()):
                return {
                    'status': 'success',
                    'paths': outputs,
                    'region': region_id,
                    'dataset': dataset
                }
            else:
                missing = [k for k, v in outputs.items() 
                          if not v.exists() or v.stat().st_size == 0]
                raise FileNotFoundError(f"Missing or empty output files: {missing}")

        except Exception as e:
            logger.error(f"Error processing {dataset} for {region_id}: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'region': region_id,
                'dataset': dataset
            }

    def serialize_paths(self, result: dict) -> dict:
        """Convert Path objects to strings for JSON serialization."""
        if result.get('status') == 'success' and 'paths' in result:
            result['paths'] = {k: str(v) for k, v in result['paths'].items()}
        return result
