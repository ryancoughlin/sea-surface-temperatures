from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List
from contextlib import contextmanager
import aiohttp
import logging
import asyncio
import xarray as xr

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

class ProcessingManager:
    """Coordinates data processing workflow"""
    
    def __init__(self, path_manager: PathManager, data_assembler: DataAssembler):
        self.path_manager = path_manager
        self.data_assembler = data_assembler
        self.session = None
        
        # Initialize processors
        self.visualizer_factory = VisualizerFactory(path_manager)
        self.geojson_converter_factory = GeoJSONConverterFactory(path_manager, data_assembler)
        self.data_preprocessor = DataPreprocessor()
        
        # Initialize services
        self.services = {}
        
    async def initialize(self, session: aiohttp.ClientSession):
        """Initialize services with session"""
        self.session = session
        self.services = {
            'erddap': ERDDAPService(session, self.path_manager),
            'cmems': CMEMSService(session, self.path_manager)
        }

    async def process_datasets(self, date: datetime, region_id: str, datasets: List[str], skip_geojson: bool = False) -> List[dict]:
        """Process multiple datasets for a region"""
        results = []
        for dataset in datasets:
            try:
                result = await self.process_dataset(date, region_id, dataset, skip_geojson)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to process {dataset}: {str(e)}")
                results.append({
                    'status': 'error',
                    'error': str(e),
                    'dataset': dataset,
                    'region': region_id
                })
        return results

    async def process_dataset(self, date: datetime, region_id: str, dataset: str, skip_geojson: bool = False) -> dict:
        """Process single dataset for a region"""
        logger.info(f"📦 Processing {dataset} for {region_id}")
        
        try:
            # Get the data file
            netcdf_path = await self._get_data(date, dataset, region_id)
            if not netcdf_path:
                return {'status': 'error', 'error': 'No data downloaded', 'dataset': dataset, 'region': region_id}

            # Process the data
            logger.info("   ├── 🔧 Processing data")
            with self._open_netcdf(netcdf_path) as ds:
                # Extract and process data
                raw_data, variables = extract_variables(ds, dataset)
                processed_data = self.data_preprocessor.preprocess_dataset(
                    data=raw_data,
                    dataset=dataset,
                    region=region_id
                )

            # Generate outputs
            result = await self._generate_outputs(
                processed_data=processed_data,
                dataset=dataset,
                region_id=region_id,
                date=date,
                skip_geojson=skip_geojson
            )
            
            logger.info("   └── ✅ Processing completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"   └── ❌ Error processing {dataset} for {region_id}: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'dataset': dataset,
                'region': region_id
            }

    @contextmanager
    def _open_netcdf(self, path: Path):
        """Open and manage NetCDF dataset"""
        ds = None
        try:
            ds = xr.open_dataset(path, decode_times=True)
            yield ds
        finally:
            if ds:
                ds.close()

    async def _get_data(self, date: datetime, dataset: str, region_id: str) -> Optional[Path]:
        """Get data file from local storage or download"""
        # Check local storage first
        local_file = self.path_manager.find_local_file(dataset, region_id, date)
        if local_file:
            return local_file
            
        # Download if not found locally
        source_type = SOURCES[dataset].get('source_type')
        if source_type not in self.services:
            raise ValueError(f"Unknown source type: {source_type}")
            
        logger.info(f"   ├── 📥 Downloading from {source_type}")
        try:
            downloaded_path = await self.services[source_type].save_data(date, dataset, region_id)
            return downloaded_path
        except Exception as e:
            logger.error(f"   └── ❌ Download failed: {str(e)}")
            return None

    async def _generate_outputs(self, processed_data, dataset: str, region_id: str, date: datetime, skip_geojson: bool) -> dict:
        """Generate all output files"""
        asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)
        dataset_type = self.data_assembler.get_dataset_config(dataset)['type']
        
        # Generate GeoJSON if needed
        if not skip_geojson:
            logger.info(f"   ├── 🗺️  Generating GeoJSON")
            geojson_converter = self.geojson_converter_factory.create(dataset, 'data')
            data_path = geojson_converter.convert(
                data=processed_data,
                region=region_id,
                dataset=dataset,
                date=date
            )
            
            # Generate contours for supported types
            if dataset_type in ['sst', 'chlorophyll']:
                logger.info(f"   ├── 📈 Generating contours")
                contour_converter = self.geojson_converter_factory.create(dataset, 'contour')
                contour_converter.convert(
                    data=processed_data,
                    region=region_id,
                    dataset=dataset,
                    date=date
                )

        # Generate image
        logger.info(f"   ├── 🖼️  Generating image")
        processor = self.visualizer_factory.create(dataset_type)
        image_path, _ = processor.generate_image(
            data=processed_data,
            region=region_id,
            dataset=dataset,
            date=date
        )

        # Update metadata
        self.data_assembler.assemble_metadata(
            data=processed_data,
            dataset=dataset,
            region=region_id,
            date=date
        )

        return {
            'status': 'success',
            'dataset': dataset,
            'region': region_id,
            'paths': {
                'data': str(asset_paths.data),
                'image': str(image_path)
            }
        }
