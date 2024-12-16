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
            source_config = SOURCES[dataset]
            source_type = source_config.get('source_type')
            logger.info(f"   ├── Source type: {source_type}")
            logger.info(f"   ├── Config: {source_config}")
            
            # Get the data file(s)
            if source_type == 'combined_view':
                logger.info(f"   ├── 📥 Processing combined view from multiple sources")
                combined_data = {}
                
                # Process each source dataset
                for source_name, source_info in source_config['source_datasets'].items():
                    logger.info(f"   ├── Processing {source_name} component")
                    logger.info(f"   │   ├── Source info: {source_info}")
                    
                    # Download using CMEMS service
                    downloaded_path = await self.services[source_info['source_type']].save_data(
                        date=date,
                        dataset=source_info['dataset_id'],
                        region=region_id,
                        variables=source_info['variables']
                    )
                    
                    if not downloaded_path:
                        logger.error(f"   │   └── Failed to download data for {source_name}")
                        return {
                            'status': 'error',
                            'error': f'Failed to download {source_name} data',
                            'dataset': dataset,
                            'region': region_id
                        }
                    
                    logger.info(f"   │   ├── Downloaded to: {downloaded_path}")
                        
                    # Load and extract variables
                    with self._open_netcdf(downloaded_path) as ds:
                        logger.info(f"   │   ├── NetCDF variables: {list(ds.variables.keys())}")
                        raw_data, variables = extract_variables(ds, source_info['dataset_id'])
                        logger.info(f"   │   ├── Extracted variables: {variables}")
                        combined_data[source_name] = {
                            'data': raw_data,
                            'variables': variables,
                            'config': source_info
                        }
                
                # Process the combined dataset
                logger.info("   ├── 🔧 Processing combined data")
                processed_data = self.data_preprocessor.preprocess_dataset(
                    data=combined_data,
                    dataset=dataset,
                    region=region_id
                )
            else:
                # Handle regular single-source datasets
                netcdf_path = await self._get_data(date, dataset, region_id)
                if not netcdf_path:
                    return {'status': 'error', 'error': 'No data downloaded', 'dataset': dataset, 'region': region_id}

                # Process the data
                logger.info("   ├── 🔧 Processing data")
                with self._open_netcdf(netcdf_path) as ds:
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
            
        # Get source configuration
        source_config = SOURCES[dataset]
        source_type = source_config.get('source_type')
        
        # Handle regular single-source datasets
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
        
        paths = {
            'data': str(asset_paths.data),
            'image': str(asset_paths.image),
            'contours': None,
            'features': None
        }
        
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
            paths['data'] = str(data_path)
            
            # Generate contours for supported types
            if dataset_type in ['sst', 'chlorophyll', 'water_movement']:
                logger.info(f"   ├── 📈 Generating contours")
                contour_converter = self.geojson_converter_factory.create(dataset, 'contour')
                contour_path = contour_converter.convert(
                    data=processed_data,
                    region=region_id,
                    dataset=dataset,
                    date=date
                )
                paths['contours'] = str(contour_path)
            
            # Generate features for water_movement
            if dataset_type == 'water_movement':
                logger.info(f"   ├── 🎯 Generating features")
                features_converter = self.geojson_converter_factory.create(dataset, 'features')
                features_path = features_converter.convert(
                    data=processed_data,
                    region=region_id,
                    dataset=dataset,
                    date=date
                )
                paths['features'] = str(features_path)

        # Generate image
        logger.info(f"   ├── 🖼️  Generating image")
        processor = self.visualizer_factory.create(dataset_type)
        image_path, _ = processor.generate_image(
            data=processed_data,
            region=region_id,
            dataset=dataset,
            date=date
        )
        paths['image'] = str(image_path)

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
            'paths': paths
        }
