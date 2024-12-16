from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Union
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
from processors.data.data_utils import standardize_dataset
from config.settings import SOURCES
from utils.path_manager import PathManager
from utils.data_utils import extract_variables

logger = logging.getLogger(__name__)

class ProcessingManager:
    """Coordinates data processing workflow"""
    
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.path_manager = PathManager(base_dir)
        self.data_assembler = DataAssembler(base_dir)
        self.session = None
        
        # Initialize processors
        self.visualizer_factory = VisualizerFactory(self.data_assembler)
        self.geojson_converter_factory = GeoJSONConverterFactory(self.path_manager, self.data_assembler)
        
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

    @contextmanager
    def _open_netcdf(self, path: Path):
        """Open and manage NetCDF dataset using netcdf4 engine."""
        ds = None
        try:
            ds = xr.open_dataset(path, engine='netcdf4', decode_times=True)
            yield ds
        finally:
            if ds:
                ds.close()

    async def _get_data(self, date: datetime, dataset: str, region_id: str) -> Optional[Path]:
        """Get data file from local storage or download"""
        local_file = self.path_manager.find_local_file(dataset, region_id, date)
        if local_file:
            logger.info(f"♻️  Using cached data for {dataset}")
            return local_file
            
        source_config = SOURCES[dataset]
        source_type = source_config.get('source_type')
        
        if source_type not in self.services:
            raise ValueError(f"Unknown source type: {source_type}")
            
        try:
            downloaded_path = await self.services[source_type].save_data(date, dataset, region_id)
            return downloaded_path
        except Exception as e:
            logger.error(f"📥 Download failed for {dataset}: {str(e)}")
            return None

    async def process_dataset(self, date: datetime, region_id: str, dataset: str, skip_geojson: bool = False) -> dict:
        """Process single dataset for a region"""
        logger.info(f"🔄 Processing {dataset} for {region_id}")
        
        try:
            source_config = SOURCES[dataset]
            source_type = source_config.get('source_type')
            
            # Get the data file(s)
            if source_type == 'combined_view':
                combined_data = {}
                
                # Process each source dataset
                for source_name, source_info in source_config['source_datasets'].items():
                    # Download using appropriate service
                    downloaded_path = await self.services[source_info['source_type']].save_data(
                        date=date,
                        dataset=source_info['dataset_id'],
                        region=region_id,
                        variables=source_info['variables']
                    )
                    
                    if not downloaded_path:
                        return {
                            'status': 'error',
                            'error': f'Failed to download {source_name} data',
                            'dataset': dataset,
                            'region': region_id
                        }
                    
                    # Load and extract variables
                    with self._open_netcdf(downloaded_path) as ds:
                        raw_data, variables = extract_variables(ds, source_info['dataset_id'])
                        combined_data[source_name] = raw_data
                
                processed_data = standardize_dataset(
                    data=combined_data,
                    dataset=dataset,
                    region=region_id
                )
            else:
                # Handle regular single-source datasets
                netcdf_path = await self._get_data(date, dataset, region_id)
                if not netcdf_path:
                    return {'status': 'error', 'error': 'No data downloaded', 'dataset': dataset, 'region': region_id}

                with self._open_netcdf(netcdf_path) as ds:
                    raw_data, variables = extract_variables(ds, dataset)
                    processed_data = standardize_dataset(
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
            
            logger.info(f"✅ Completed {dataset} for {region_id}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed {dataset} for {region_id}: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'dataset': dataset,
                'region': region_id
            }

    async def _generate_outputs(self, processed_data, dataset: str, region_id: str, date: datetime, skip_geojson: bool) -> dict:
        """Generate all output files"""
        asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)
        
        paths = {
            'data': str(asset_paths.data),
            'image': str(asset_paths.image),
            'contours': str(asset_paths.contours),
            'features': str(asset_paths.features)
        }
        
        dataset_config = SOURCES[dataset]
        
        if not skip_geojson:
            paths.update(self._generate_geojson_layers(
                data=processed_data,
                dataset=dataset,
                dataset_type=dataset_config['type'],
                region_id=region_id,
                date=date,
                asset_paths=asset_paths
            ))

        processor = self.visualizer_factory.create(dataset_config['type'])
        image_path = processor.save_image(
            data=processed_data,
            region=region_id,
            dataset=dataset,
            date=date,
            asset_paths=asset_paths
        )
        paths['image'] = str(image_path)

        self.data_assembler.update_metadata(
            dataset=dataset,
            region=region_id,
            date=date,
            paths=paths
        )

        return {
            'status': 'success',
            'dataset': dataset,
            'region': region_id,
            'paths': paths
        }

    def _generate_geojson_layers(self, data, dataset: str, dataset_type: str, 
                               region_id: str, date: datetime, asset_paths) -> Dict[str, str]:
        """Generate GeoJSON layers based on dataset type."""
        paths = {}
        
        # Generate base data layer
        logger.info(f"🗺️  Generating data layer")
        
        geojson_converter = self.geojson_converter_factory.create(dataset, 'data')
        data_path = geojson_converter.convert(
            data=data,
            region=region_id,
            dataset=dataset,
            date=date
        )
        paths['data'] = str(data_path)
        
        # Generate additional layers based on dataset type
        if dataset_type in ['sst', 'chlorophyll', 'water_movement']:
            logger.info(f"📈 Generating contours")
            contour_converter = self.geojson_converter_factory.create(dataset, 'contours')
            contour_path = contour_converter.convert(
                data=data,
                region=region_id,
                dataset=dataset,
                date=date
            )
            paths['contours'] = str(contour_path)
            
            if dataset_type == 'water_movement':
                logger.info(f"🎯 Generating features")
                features_converter = self.geojson_converter_factory.create(dataset, 'features')
                features_path = features_converter.convert(
                    data=data,
                    region=region_id,
                    dataset=dataset,
                    date=date
                )
                paths['features'] = str(features_path)
                
        return paths
