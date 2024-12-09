from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from contextlib import contextmanager
import aiohttp
import logging
import asyncio
import xarray as xr
import re
import pandas as pd

from services.erddap_service import ERDDAPService
from services.cmems_service import CMEMSService
from processors.metadata_assembler import MetadataAssembler
from processors.geojson.factory import GeoJSONConverterFactory
from processors.processor_factory import ProcessorFactory
from processors.data_preprocessor import DataPreprocessor
from processors.cleanup_manager import CleanupManager
from config.settings import SOURCES
from utils.path_manager import PathManager
from processors.data_cleaners.land_masker import LandMasker
from processors.cache_manager import CacheManager

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
    
    def __init__(self, path_manager: PathManager, metadata_assembler: MetadataAssembler):
        self.path_manager = path_manager
        self.metadata_assembler = metadata_assembler
        self.session = None
        self.erddap_service = None
        self.cmems_service = None
        self.podaac_service = None
        
        # Initialize services and managers
        self.processor_factory = ProcessorFactory(path_manager)
        self.geojson_converter_factory = GeoJSONConverterFactory(path_manager, metadata_assembler)
        self.data_preprocessor = DataPreprocessor()
        self.cache_manager = CacheManager()
        self.cleanup_manager = CleanupManager(path_manager)
        self.logger = logging.getLogger(__name__)
        
        self.land_masker = LandMasker()
        
    async def initialize(self, session: aiohttp.ClientSession):
        """Initialize services with session"""
        self.session = session
        self.erddap_service = ERDDAPService(session, self.path_manager)
        self.cmems_service = CMEMSService(session, self.path_manager)

    @contextmanager
    def managed_netcdf(self, path: Path, cleanup: bool = True):
        """Manage NetCDF dataset lifecycle"""
        ds = None
        try:
            ds = xr.open_dataset(path, decode_times=True)
            yield ds
        finally:
            if ds:
                ds.close()
            if cleanup and path.exists():
                path.unlink()

    async def process_dataset(self, date: datetime, region_id: str, dataset: str, skip_geojson: bool = False) -> dict:
        """Process single dataset for a region"""
        try:
            # Perform cleanup before processing new data
            self.cleanup_manager.cleanup()
            
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

    def _extract_time_from_file(self, file_path: Path) -> Optional[datetime]:
        """Extract time from PODAAC filename or metadata"""
        try:
            # First try to get from filename
            # Expected format: GOES16_SST_OSISAF_L3C_YYYYMMDD_HH.nc
            match = re.search(r'(\d{8})_(\d{2})\.nc$', file_path.name)
            if match:
                date_str, hour_str = match.groups()
                return datetime.strptime(f"{date_str}{hour_str}", '%Y%m%d%H')
            
            # If not in filename, try to read from NetCDF metadata
            ds = xr.open_dataset(file_path)
            if 'time' in ds.dims:
                return pd.to_datetime(ds.time.values[0])
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting time from file: {str(e)}")
            return None

    async def _get_data(self, date: datetime, dataset: str, region_id: str, cache_path: Path) -> Optional[Path]:
        """Get data from cache or download"""
        # Check cache first
        cached_file = self.cache_manager.get_cached_file(dataset, region_id, date)
        if cached_file:
            logger.info(f"   ├── ✅ Using cached file: {cached_file.name}")
            return cached_file
            
        # If not in cache, download
        source_type = SOURCES[dataset].get('source_type')
        logger.info(f"   ├── 📥 Downloading from {source_type}")
        
        try:
            if source_type == 'cmems':
                downloaded_path = await self.cmems_service.save_data(date, dataset, region_id)
            elif source_type == 'erddap':
                downloaded_path = await self.erddap_service.save_data(date, dataset, region_id)
            else:
                logger.error(f"   └── ❌ Unknown source type: {source_type}")
                raise ProcessingError("download", f"Unknown source type: {source_type}",
                                   {"dataset": dataset, "region": region_id})
                
            # Save to cache if download successful
            if downloaded_path:
                return self.cache_manager.save_to_cache(downloaded_path, dataset, region_id, date)
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
            dataset_type = SOURCES[dataset]['type']
            variables = SOURCES[dataset]['variables']
            source = SOURCES[dataset].get('source', '')
            
            # Special handling for PODAAC data which has multiple hourly files
            if source == 'podaac':
                results = []
                for file_path in [netcdf_path] if isinstance(netcdf_path, Path) else netcdf_path:
                    file_date = self._extract_datetime_from_filename(file_path.name)
                    if not file_date:
                        logger.warning(f"Could not extract date from {file_path.name}")
                        continue
                        
                    result = await self._process_single_netcdf(
                        file_path, region_id, dataset, file_date, skip_geojson
                    )
                    if result['status'] == 'success':
                        results.append(result)
                
                if not results:
                    logger.error("   └── ❌ No files processed successfully")
                    return {
                        'status': 'error',
                        'error': 'No files processed successfully',
                        'region': region_id,
                        'dataset': dataset
                    }
                
                logger.info(f"   └── ✅ Processed {len(results)} files successfully")
                return {
                    'status': 'success',
                    'processed_files': len(results),
                    'results': results,
                    'region': region_id,
                    'dataset': dataset
                }
            
            # Standard processing for non-PODAAC data
            return await self._process_single_netcdf(netcdf_path, region_id, dataset, date, skip_geojson)
            
        except Exception as e:
            logger.error(f"   └── ❌ Processing failed: {str(e)}")
            raise ProcessingError("processing", str(e), 
                                {"dataset": dataset, "region": region_id}) from e

    def _extract_datetime_from_filename(self, filename: str) -> Optional[datetime]:
        """Extract datetime from GOES16 SST filename format."""
        # Expected format: YYYYMMDDHHMMSS-OSISAF-L3C_GHRSST...
        match = re.match(r'(\d{8})(\d{6})', filename)
        if match:
            date_str, time_str = match.groups()
            try:
                return datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
            except ValueError:
                return None
        return None

    async def _process_single_netcdf(self, netcdf_path: Path, region_id: str, dataset: str, date: datetime, skip_geojson: bool = False):
        """Process a single netCDF file."""
        try:
            # Get asset paths and dataset type
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)
            dataset_type = SOURCES[dataset]['type']
            variables = SOURCES[dataset]['variables']
            
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
                # Load all required variables from dataset
                if isinstance(variables, list) and len(variables) > 1:
                    # Multiple variables (e.g. currents with u,v)
                    raw_data = xr.Dataset({
                        var: ds[var] for var in variables
                    })
                    logger.info(f"   ├── 📥 Loaded variables: {', '.join(variables)}")
                else:
                    # Single variable
                    var_name = variables[0] if isinstance(variables, list) else variables
                    raw_data = ds[var_name]
                    logger.info(f"   ├── 📥 Loaded variable: {var_name}")
                
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
            processor = self.processor_factory.create(dataset_type)
            image_path, additional_layers = processor.generate_image(
                data=processed_data,
                region=region_id,
                dataset=dataset,
                date=date
            )
            logger.info(f"   ├── ✅ Image generated")
            
            # Update metadata
            logger.info(f"   ├── 📝 Updating metadata")
            self.metadata_assembler.assemble_metadata(
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

    def _handle_error(self, e: Exception, dataset: str, region_id: str) -> Dict:
        """Unified error handler"""
        if isinstance(e, ProcessingError):
            error_type = e.step
            error_msg = e.error
            context = e.context
        else:
            error_type = e.__class__.__name__
            error_msg = str(e)
            context = {"dataset": dataset, "region": region_id}

        logger.error(f"Error processing {dataset} for {region_id}")
        logger.error(f"Type: {error_type}")
        logger.error(f"Error: {error_msg}")

        return {
            'status': 'error',
            'error': error_msg,
            'error_type': error_type,
            **context
        }
