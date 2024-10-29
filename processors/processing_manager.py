from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Union
from services.erddap_service import ERDDAPService
from processors.metadata_assembler import MetadataAssembler
from processors.geojson.factory import GeoJSONConverterFactory
from processors.processor_factory import ProcessorFactory
from config.settings import SOURCES
from config.regions import REGIONS
import logging
from utils.file_checker import check_existing_data
from utils.path_manager import PathManager

logger = logging.getLogger(__name__)

class ProcessingManager:
    """Coordinates data processing workflow using existing services"""
 
    def __init__(self, path_manager: PathManager, metadata_assembler: MetadataAssembler):
        """Initialize with core dependencies"""
        self.path_manager = path_manager
        self.metadata_assembler = metadata_assembler
        self.session = None
        self.erddap_service = None
        
        # Factories
        self.processor_factory = ProcessorFactory(path_manager)
        self.geojson_converter_factory = GeoJSONConverterFactory(path_manager)

    async def initialize(self, session):
        """Initialize async services"""
        self.session = session
        self.erddap_service = ERDDAPService(session, self.path_manager)

    async def process_dataset(self, date: datetime, region_id: str, dataset: str) -> Dict[str, Union[str, Path, dict]]:
        """Process a single dataset for a region."""
        if not self.erddap_service:
            raise RuntimeError("ProcessingManager not initialized. Call initialize() first.")
            
        try:
            region = REGIONS[region_id]
            dataset_config = SOURCES[dataset]

            # Get paths using PathManager
            data_path = self.path_manager.get_data_path(date, dataset, region_id)
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)

            # Check for existing NetCDF data
            existing_data = check_existing_data(
                path=data_path,
                region=region,
                dataset_id=dataset_config['dataset_id'],
                date=date
            )
            
            if existing_data:
                logger.info(f"Using existing data file: {existing_data}")
                netcdf_path = existing_data
            else:
                # If no data exists, download it
                netcdf_path = await self.erddap_service.save_data(
                    date=date,
                    dataset=dataset_config,
                    region_id=region_id
                )

            # Process the netCDF file
            try:
                # Generate base GeoJSON data
                geojson_converter = self.geojson_converter_factory.create(dataset, 'data')
                geojson_path = geojson_converter.convert(
                    data_path=netcdf_path,
                    region=region_id,
                    dataset=dataset,
                    date=date
                )

                # Generate image
                processor = self.processor_factory.create(dataset)
                logger.info(f"Processing {dataset} data for {region_id}")
                image_path = processor.generate_image(
                    data_path=netcdf_path,
                    region=region_id,
                    dataset=dataset,
                    date=date
                )

                metadata_path: Path = self.metadata_assembler.assemble_metadata(
                    region=region_id,
                    dataset=dataset,
                    date=date,
                    asset_paths=asset_paths
                )
                logger.info(f"Metadata saved at {metadata_path}")

                return {
                    'status': 'success',
                    'paths': asset_paths._asdict(),
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
