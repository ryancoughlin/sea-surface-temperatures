from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List
from contextlib import contextmanager
import aiohttp
import logging
import xarray as xr
import numpy as np
import os

from services.erddap_service import ERDDAPService
from services.cmems_service import CMEMSService
from processors.data.data_assembler import DataAssembler
from processors.geojson.factory import GeoJSONConverterFactory
from processors.visualization.visualizer_factory import VisualizerFactory
from config.settings import SOURCES, PATHS
from utils.path_manager import PathManager
from processors.data.data_preprocessor import DataPreprocessor
from config.regions import REGIONS

logger = logging.getLogger(__name__)

class ProcessingManager:
    """Coordinates data processing workflow"""
    
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.path_manager = PathManager(base_dir)
        self.data_assembler = DataAssembler(base_dir)
        self.data_preprocessor = DataPreprocessor()
        self.session = None
        
        # Initialize processors
        self.visualizer_factory = VisualizerFactory(self.data_assembler)
        self.geojson_converter_factory = GeoJSONConverterFactory(self.path_manager, self.data_assembler)
        
        # Initialize services
        self.services = {}

    async def _get_data(self, date: datetime, dataset: str, region_id: str) -> Optional[Path]:
        """Get data file from local storage or download"""
        # Ensure the download directory exists
        download_dir = PATHS['DOWNLOADED_DATA_DIR'] / region_id / dataset / date.strftime('%Y%m%d')
        self.ensure_directory(download_dir)
        
        # Check downloaded data first
        downloaded_path = download_dir / 'raw.nc'
        if downloaded_path.exists():
            logger.info(f"♻️  Using cached data for {dataset}")
            return downloaded_path
            
        source_config = SOURCES[dataset]
        source_type = source_config.get('source_type')
        
        if source_type not in self.services:
            raise ValueError(f"Unknown source type: {source_type}")
            
        try:
            logger.info(f"📥 Starting download for {dataset} using {source_type} service")
            downloaded_path = await self.services[source_type].save_data(date, dataset, region_id)
            logger.info(f"✅ Download completed for {dataset}, saved to {downloaded_path}")
            return downloaded_path
        except Exception as e:
            logger.error(f"📥 Download failed for {dataset}: {str(e)}")
            return None

    async def initialize(self, session: aiohttp.ClientSession):
        """Initialize services with session"""
        self.session = session
        self.services = {
            'erddap': ERDDAPService(session, self.path_manager),
            'cmems': CMEMSService(session, self.path_manager)
        }
        
        # Ensure base directories exist
        for path in [PATHS['DATA_DIR'], PATHS['DOWNLOADED_DATA_DIR']]:
            self.ensure_directory(path)

    def ensure_directory(self, path: Path):
        """Create directory if it doesn't exist and ensure proper permissions."""
        path.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(path, 0o777)
        except Exception as e:
            logger.warning(f"Could not set permissions for {path}: {e}")

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

    async def process_dataset(self, date: datetime, region_id: str, dataset: str, skip_geojson: bool = False) -> dict:
        """Process single dataset for a region"""
        logger.info(f"🔄 Processing {dataset} for {region_id}")
        
        try:
            source_config = SOURCES[dataset]
            source_type = source_config.get('source_type')
            dataset_type = source_config.get('type')
            
            # Get the data file(s)
            if source_type == 'combined_view':
                # Process each source dataset
                datasets_to_merge = []
                logger.info(f"Processing combined dataset with sources: {list(source_config['source_datasets'].keys())}")
                
                for source_name, source_info in source_config['source_datasets'].items():
                    logger.info(f"Processing {source_name} component of {dataset}")
                    
                    # Ensure directory exists for component dataset
                    download_dir = PATHS['DOWNLOADED_DATA_DIR'] / region_id / source_info['dataset_id'] / date.strftime('%Y%m%d')
                    self.ensure_directory(download_dir)
                    
                    # Download using appropriate service
                    try:
                        downloaded_path = await self.services[source_info['source_type']].save_data(
                            date=date,
                            dataset=source_info['dataset_id'],
                            region=region_id,
                            variables=list(source_info['variables'].keys())
                        )
                        
                        if not downloaded_path:
                            raise ValueError(f"No data downloaded for {source_name}")
                            
                        # Load and process data
                        with self._open_netcdf(downloaded_path) as ds:
                            processed_data = self.data_preprocessor.preprocess_dataset(data=ds, dataset_type=dataset_type)
                            datasets_to_merge.append(processed_data)
                            
                    except Exception as e:
                        logger.error(f"Failed to process {source_name} component: {str(e)}")
                        return {
                            'status': 'error',
                            'error': f'Failed to process {source_name} component: {str(e)}',
                            'dataset': dataset,
                            'region': region_id
                        }
                
                # Merge preprocessed datasets
                processed_data = xr.merge(datasets_to_merge)
            else:
                # Handle regular single-source datasets
                netcdf_path = await self._get_data(date, dataset, region_id)
                if not netcdf_path:
                    return {'status': 'error', 'error': 'No data downloaded', 'dataset': dataset, 'region': region_id}

                with self._open_netcdf(netcdf_path) as ds:
                    processed_data = self.data_preprocessor.preprocess_dataset(data=ds, dataset_type=dataset_type)
                    
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

    def _calculate_data_ranges(self, data: xr.Dataset) -> Dict:
        """Calculate data ranges for all variables in dataset."""
        ranges = {}
        
        for var in data.data_vars:
            values = data[var].values
            valid_data = values[~np.isnan(values)]
            if len(valid_data) > 0:
                ranges[var] = {
                    'min': float(np.min(valid_data)),
                    'max': float(np.max(valid_data)),
                    'unit': data[var].attrs.get('units', '')
                }
                logger.info(f"[RANGES] {var} min/max: {ranges[var]['min']:.4f} to {ranges[var]['max']:.4f}")
            
        return ranges

    async def _generate_outputs(self, processed_data, dataset: str, region_id: str, date: datetime, skip_geojson: bool) -> dict:
        """Generate all output files and update metadata."""
        try:
            dataset_config = SOURCES[dataset]
            dataset_type = dataset_config['type']
            
            # Get paths for this dataset
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)
            
            paths = {
                'data': str(asset_paths.data),
                'image': str(asset_paths.image),
                'contours': str(asset_paths.contours),
                'features': str(asset_paths.features)
            }
            
            # Calculate data ranges for metadata
            ranges = {}
            if isinstance(processed_data, dict):
                # Handle combined datasets
                for component_name, component_data in processed_data.items():
                    if isinstance(component_data, (xr.Dataset, xr.DataArray)):
                        ranges[component_name] = self._calculate_data_ranges(component_data)
            else:
                # Handle single datasets
                ranges = self._calculate_data_ranges(processed_data)

            # Generate GeoJSON layers if not skipped
            if not skip_geojson:
                paths.update(self._generate_geojson_layers(
                    data=processed_data,
                    dataset=dataset,
                    dataset_type=dataset_type,
                    region_id=region_id,
                    date=date,
                    asset_paths=asset_paths
                ))
                
            # Generate image layer
            logger.info(f"🎨 Generating image layer")
            processor = self.visualizer_factory.create(dataset_config['type'])
            image_path = processor.save_image(
                data=processed_data,
                region=region_id,
                dataset=dataset,
                date=date,
                asset_paths=asset_paths
            )
            paths['image'] = str(image_path)

            # Update metadata with file paths
            self.data_assembler.update_metadata(
                dataset=dataset,
                region=region_id,
                date=date,
                paths=paths,
                ranges=ranges
            )

            return {
                'status': 'success',
                'dataset': dataset,
                'region': region_id,
                'paths': paths,
                'ranges': ranges
            }
        except Exception as e:
            logger.error(f"Error generating outputs: {e}")
            raise

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
        if data_path and data_path.exists():
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
            if contour_path and contour_path.exists():
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
                if features_path and features_path.exists():
                    paths['features'] = str(features_path)

        return paths
