from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Union
from services.erddap_service import ERDDAPService
from processors.tile_generator import TileGenerator
from processors.metadata_assembler import MetadataAssembler
from processors.geojson.factory import GeoJSONConverterFactory
from processors.processor_factory import ProcessorFactory
from config.settings import OUTPUT_DIR, SOURCES, REGIONS_DIR
from config.regions import REGIONS
import logging

logger = logging.getLogger(__name__)

class ProcessingManager:
    """Coordinates data processing workflow using existing services"""
    
    def __init__(self, metadata_assembler: MetadataAssembler):
        """Initialize with required services"""
        # Core services
        self.metadata_assembler = metadata_assembler
        self.session = None
        self.erddap_service = None
        
        # Factories
        self.processor_factory = ProcessorFactory()
        self.geojson_converter_factory = GeoJSONConverterFactory()

    def start_session(self, session):
        self.session = session
        self.erddap_service = ERDDAPService(session)  # Create service with session

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

            # Check for existing GeoJSON
            expected_geojson_path = Path(OUTPUT_DIR) / region_id / "datasets" / dataset / timestamp / "data.geojson"
            
            if expected_geojson_path.exists():
                logger.info(f"GeoJSON file already exists: {expected_geojson_path}")
                return {
                    'status': 'success',
                    'paths': {
                        'geojson': expected_geojson_path
                    },
                    'region': region_id,
                    'dataset': dataset
                }

            # If no GeoJSON exists, get the netCDF data and process it
            try:
                netcdf_path: Path = await self.erddap_service.save_data(
                    date=date,
                    dataset=dataset_config,
                    region=region,
                    output_path=OUTPUT_DIR
                )
                
                if not netcdf_path.exists():
                    raise FileNotFoundError(f"NetCDF file not found for {region_id}, {dataset}")

                # Process the netCDF file
                geojson_converter = self.geojson_converter_factory.create(dataset, 'data')
                geojson_path = geojson_converter.convert(
                    data_path=netcdf_path,  # Use the netCDF path here
                    region=region_id,
                    dataset=dataset,
                    timestamp=timestamp
                )

                processor = ProcessorFactory.create(dataset)
                logger.info(f"Processing {dataset} data for {region_id}")

                processing_result = processor.generate_image(
                    data_path=netcdf_path,
                    region=region_id,
                    dataset=dataset,
                    timestamp=timestamp
                )

                if isinstance(processing_result, tuple):
                    image_path, additional_layers = processing_result
                else:
                    image_path = processing_result
                    additional_layers = None

                if SOURCES[dataset].get('type') == 'sst':
                    contour_converter = self.geojson_converter_factory.create(dataset, 'contours')
                    contour_path = contour_converter.convert(
                        data_path=netcdf_path,
                        region=region_id,
                        dataset=dataset,
                        timestamp=timestamp
                    )
                    additional_layers = {
                        "contours": {
                            "layers": str(contour_path.relative_to(REGIONS_DIR)),
                        }
                    }

                metadata_path: Path = self.metadata_assembler.assemble_metadata(
                    region=region_id,
                    dataset=dataset,
                    timestamp=timestamp,
                    image_path=image_path,
                    geojson_path=geojson_path,
                    additional_layers=additional_layers
                )
                logger.info(f"Metadata saved at {metadata_path}")

                return {
                    'status': 'success',
                    'paths': {
                        'data': netcdf_path,
                        'image': image_path,
                        'geojson': geojson_path,
                        'metadata': metadata_path
                    },
                    'region': region_id,
                    'dataset': dataset
                }

            except Exception as e:
                logger.error(f"Error processing {dataset} for {region_id}: {str(e)}")
                return {
                    'status': 'error',
                    'error': str(e),
                    'region': region_id,
                    'dataset': dataset
                }

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
