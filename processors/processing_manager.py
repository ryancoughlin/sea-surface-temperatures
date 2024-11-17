from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
import aiohttp
from services.erddap_service import ERDDAPService
from services.cmems_service import CMEMSService
from processors.metadata_assembler import MetadataAssembler
from processors.geojson.factory import GeoJSONConverterFactory
from processors.processor_factory import ProcessorFactory
from config.settings import SOURCES
from config.regions import REGIONS
import logging
from utils.file_checker import check_existing_data
from utils.path_manager import PathManager
import asyncio

logger = logging.getLogger(__name__)

class ProcessingManager:
    """Coordinates data processing workflow using existing services"""
 
    def __init__(self, path_manager: PathManager, metadata_assembler: MetadataAssembler):
        """Initialize with core dependencies"""
        self.path_manager = path_manager
        self.metadata_assembler = metadata_assembler
        self.session = None
        self.erddap_service = None
        self.cmems_service = None
        
        # Factories
        self.processor_factory = ProcessorFactory(path_manager)
        self.geojson_converter_factory = GeoJSONConverterFactory(path_manager)

    async def initialize(self, session: aiohttp.ClientSession):
        """Initialize services with session"""
        self.session = session
        self.erddap_service = ERDDAPService(session, self.path_manager)
        self.cmems_service = CMEMSService(session, self.path_manager)

    async def process_dataset(self, date: datetime, region_id: str, dataset: str) -> Dict:
        """Process a single dataset"""
        if not self.session:
            raise RuntimeError("ProcessingManager not initialized")
        
        try:
            logger.info(
                "Processing dataset\n"
                f"Dataset: {dataset}\n"
                f"Region:  {region_id}\n"
                f"Date:    {date.strftime('%Y-%m-%d')}"
            )
            
            # Get paths
            data_path = self.path_manager.get_data_path(date, dataset, region_id)
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)

            # Use cached data if available
            if data_path.exists():
                logger.info(f"Using cached data: {data_path}")
                return await self._process_netcdf_data(
                    data_path, region_id, dataset, date, asset_paths
                )

            # Download new data
            service = self.cmems_service if SOURCES[dataset].get('source_type') == 'cmems' else self.erddap_service
            netcdf_path = await service.save_data(date, dataset, region_id)
            
            if not netcdf_path:
                return {
                    'status': 'error',
                    'error': 'Failed to download data',
                    'region': region_id,
                    'dataset': dataset
                }
            
            # Process the data
            return await self._process_netcdf_data(
                netcdf_path, region_id, dataset, date, asset_paths
            )

        except Exception as e:
            logger.error(
                "Processing error\n"
                f"Dataset: {dataset}\n"
                f"Region:  {region_id}\n"
                f"Error:   {str(e)}"
            )
            return {
                'status': 'error',
                'error': str(e),
                'region': region_id,
                'dataset': dataset
            }

    async def _process_netcdf_data(self, netcdf_path: Path, region_id: str, dataset: str, date: datetime, asset_paths: Path):
        """Process the downloaded netCDF data."""
        try:
            # Generate base GeoJSON data
            geojson_converter = self.geojson_converter_factory.create(dataset, 'data')
            data_path = geojson_converter.convert(
                data_path=netcdf_path,
                region=region_id,
                dataset=dataset,
                date=date
            )

            # Generate contours for SST data
            if SOURCES[dataset]['type'] == 'sst':
                contour_converter = self.geojson_converter_factory.create(dataset, 'contour')
                contour_path = contour_converter.convert(
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

            logger.info(
                "Processing completed\n"
                f"Dataset: {dataset}\n"
                f"Region:  {region_id}\n"
                f"Output:  {data_path}"
            )

            return {
                'status': 'success',
                'paths': asset_paths._asdict(),
                'region': region_id,
                'dataset': dataset
            }
        except Exception as e:
            logger.error(f"Error in _process_netcdf_data: {str(e)}")
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
