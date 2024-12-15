from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List
from contextlib import contextmanager
import aiohttp
import logging
import asyncio
import xarray as xr
import re

from services.erddap_service import ERDDAPService
from services.cmems_service import CMEMSService
from processors.data.data_assembler import DataAssembler
from processors.geojson.factory import GeoJSONConverterFactory
from processors.visualization.visualizer_factory import VisualizerFactory
from processors.data.data_preprocessor import DataPreprocessor
from config.settings import SOURCES
from utils.path_manager import PathManager
from utils.data_utils import extract_variables

logger = logging.getLogger(__name__)

class ProcessingError(Exception):
    """Custom error for processing failures"""
    def __init__(self, step: str, error: str, context: dict):
        self.step = step
        self.error = error
        self.context = context
        super().__init__(f"{step}: {error}")

class ProcessingManager:
    """Coordinates data processing workflow"""
    
    def __init__(self, path_manager: PathManager, data_assembler: DataAssembler):
        self.path_manager = path_manager
        self.data_assembler = data_assembler
        self.session = None
        self.erddap_service = None
        self.cmems_service = None
        
        # Initialize factories
        self.visualizer_factory = VisualizerFactory(path_manager)
        self.geojson_converter_factory = GeoJSONConverterFactory(path_manager, data_assembler)
        self.data_preprocessor = DataPreprocessor()
        
    async def initialize(self, session: aiohttp.ClientSession):
        """Initialize services with session"""
        self.session = session
        
    def _get_service(self, source_type: str):
        """Get or initialize service on demand"""
        if source_type == 'erddap':
            if not self.erddap_service:
                self.erddap_service = ERDDAPService(self.session, self.path_manager)
            return self.erddap_service
        elif source_type == 'cmems':
            if not self.cmems_service:
                self.cmems_service = CMEMSService(self.session, self.path_manager)
            return self.cmems_service
        else:
            raise ValueError(f"Unknown source type: {source_type}")
        
    async def process_datasets(self, date: datetime, region_id: str, datasets: List[str], skip_geojson: bool = False) -> List[dict]:
        """Process multiple datasets for a region"""
        results = []
        for dataset in datasets:
            result = await self.process_dataset(date, region_id, dataset, skip_geojson)
            results.append(result)
        return results

    async def process_dataset(self, date: datetime, region_id: str, dataset: str, skip_geojson: bool = False) -> dict:
        """Process single dataset for a region"""
        try:
            # Get paths
            data_path = self.path_manager.get_data_path(date, dataset, region_id)
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)

            # Use cached or download new data
            logger.info(f"📦 Processing {dataset} for {region_id}")
            source_type = SOURCES[dataset].get('source_type')

            netcdf_path = await self._get_data(date, dataset, region_id, data_path)
            if not netcdf_path:
                logger.error("   └── ❌ No data downloaded")
                return {
                    'status': 'error',
                    'error': 'No data downloaded',
                    'region': region_id,
                    'dataset': dataset
                }
            
            # Process the data
            logger.info("   ├── 🔧 Processing data")
            result = await self._process_netcdf_data(
                netcdf_path, region_id, dataset, date, skip_geojson
            )
            if result['status'] == 'success':
                logger.info("   └── ✅ Processing completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"   └── ❌ Error processing {dataset} for {region_id}: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'region': region_id,
                'dataset': dataset
            }

    @contextmanager
    def managed_netcdf(self, path: Path):
        """Manage NetCDF dataset lifecycle"""
        ds = None
        try:
            ds = xr.open_dataset(path, decode_times=True)
            yield ds
        finally:
            if ds:
                ds.close()

    async def _get_data(self, date: datetime, dataset: str, region_id: str, cache_path: Path) -> Optional[Path]:
        """Get data from cache or download"""
        # Check cache first
        cached_file = self.path_manager.get_cached_file(dataset, region_id, date)
        if cached_file:
            return cached_file
            
        # If not in cache, download
        source_type = SOURCES[dataset].get('source_type')
        logger.info(f"   ├── 📥 Downloading from {source_type}")
        
        try:
            service = self._get_service(source_type)
            downloaded_path = await service.save_data(date, dataset, region_id)
                
            # Save to cache if download successful
            if downloaded_path:
                return self.path_manager.save_to_cache(downloaded_path, dataset, region_id, date)
            return None
                
        except asyncio.CancelledError:
            logger.warning(f"   └── ⚠️ Task cancelled for {dataset}")
            raise
        except Exception as e:
            logger.error(f"   └── ❌ Download failed: {str(e)}")
            raise ProcessingError("download", str(e), 
                                {"dataset": dataset, "region": region_id}) from e

    async def _process_netcdf_data(self, netcdf_path: Path, region_id: str, dataset: str, date: datetime, skip_geojson: bool = False):
        """Process the downloaded netCDF data."""
        try:
            # Get asset paths and dataset type
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)
            dataset_config = self.data_assembler.get_dataset_config(dataset)
            dataset_type = dataset_config['type']
            
            # Check if we already have processed data for this date/time
            if all(path.exists() for path in [asset_paths.data, asset_paths.image]):
                logger.info(f"   ├── ✅ Found cached data for {dataset} at {date}")
                return {
                    'status': 'success',
                    'dataset': dataset,
                    'region': region_id,
                    'paths': {
                        'data': str(asset_paths.data),
                        'image': str(asset_paths.image)
                    }
                }
            
            # Load and process data
            logger.info(f"   ├── 📊 Processing {dataset_type} data")
            with self.managed_netcdf(netcdf_path) as ds:
                # Extract variables using data_utils
                raw_data, variables = extract_variables(ds, dataset)
                logger.info(f"   ├── 📥 Loaded data for processing")
                
                # Preprocess data
                processed_data = self.data_preprocessor.preprocess_dataset(
                    data=raw_data,
                    dataset=dataset,
                    region=region_id
                )
            
            # Generate assets using processed data
            if not skip_geojson:
                # Base GeoJSON
                logger.info(f"   ├── 🗺️  Generating GeoJSON")
                geojson_converter = self.geojson_converter_factory.create(dataset, 'data')
                data_path = geojson_converter.convert(
                    data=processed_data,
                    region=region_id,
                    dataset=dataset,
                    date=date
                )
                logger.info(f"   ├── ✅ GeoJSON generated")

                # Contours for supported types
                if dataset_type in ['sst', 'chlorophyll']:
                    logger.info(f"   ├── 📈 Generating contours")
                    contour_converter = self.geojson_converter_factory.create(dataset, 'contour')
                    contour_path = contour_converter.convert(
                        data=processed_data,
                        region=region_id,
                        dataset=dataset,
                        date=date
                    )
                    logger.info(f"   ├── ✅ Contours generated")

            # Image
            logger.info(f"   ├── 🖼️  Generating image")
            processor = self.visualizer_factory.create(dataset_type)
            image_path, additional_layers = processor.generate_image(
                data=processed_data,
                region=region_id,
                dataset=dataset,
                date=date
            )
            logger.info(f"   ├── ✅ Image generated")
            
            # Update metadata
            logger.info(f"   ├── 📝 Updating metadata")
            self.data_assembler.assemble_metadata(
                data=processed_data,
                dataset=dataset,
                region=region_id,
                date=date
            )
            logger.info(f"   └── ✅ All processing completed")
            
            return {
                'status': 'success',
                'dataset': dataset,
                'region': region_id,
                'paths': {
                    'data': str(asset_paths.data),
                    'image': str(image_path)
                }
            }
            
        except Exception as e:
            logger.error(f"   └── ❌ Processing failed: {str(e)}")
            raise ProcessingError("processing", str(e), 
                                {"dataset": dataset, "region": region_id}) from e
