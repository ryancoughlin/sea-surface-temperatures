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
from services.podaac_service import PodaacService
from processors.metadata_assembler import MetadataAssembler
from processors.geojson.factory import GeoJSONConverterFactory
from processors.processor_factory import ProcessorFactory
from processors.data_preprocessor import DataPreprocessor
from config.settings import SOURCES
from utils.path_manager import PathManager
from processors.data_cleaners.land_masker import LandMasker

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
        
        # Initialize factories and processors
        self.processor_factory = ProcessorFactory(path_manager)
        self.geojson_converter_factory = GeoJSONConverterFactory(path_manager, metadata_assembler)
        self.data_preprocessor = DataPreprocessor()
        self.logger = logging.getLogger(__name__)
        
        self.land_masker = LandMasker()
        
    async def initialize(self, session: aiohttp.ClientSession):
        """Initialize services with session"""
        self.session = session
        self.erddap_service = ERDDAPService(session, self.path_manager)
        self.cmems_service = CMEMSService(session, self.path_manager)
        self.podaac_service = PodaacService(session, self.path_manager)

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

    async def process_dataset(self, date: datetime, region_id: str, dataset: str) -> dict:
        """Process single dataset for a region"""
        try:
            # Get paths
            data_path = self.path_manager.get_data_path(date, dataset, region_id)
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)

            # Use cached or download new data
            logger.info("   └── 🔄 Fetching data...")
            source_type = SOURCES[dataset].get('source_type')
            
            if source_type == 'podaac':
                # Handle PODAAC data differently - returns multiple files
                netcdf_paths = await self._get_data(date, dataset, region_id, data_path)
                if not netcdf_paths:
                    return {
                        'status': 'error',
                        'error': 'No data downloaded',
                        'region': region_id,
                        'dataset': dataset
                    }
                
                # Process each file
                results = []
                for netcdf_path in netcdf_paths:
                    try:
                        # Extract time from filename or metadata
                        file_time = self._extract_time_from_file(netcdf_path)
                        if not file_time:
                            logger.warning(f"Could not extract time from {netcdf_path}")
                            continue
                            
                        # Get asset paths for this specific time
                        time_asset_paths = self.path_manager.get_asset_paths(file_time, dataset, region_id)
                        
                        # Process the file
                        logger.info(f"   └── 🔧 Processing file for {file_time}")
                        result = await self._process_netcdf_data(
                            netcdf_path, region_id, dataset, file_time
                        )
                        if result['status'] == 'success':
                            results.append(result)
                            
                    except Exception as e:
                        logger.error(f"Error processing file {netcdf_path}: {str(e)}")
                        continue
                
                if not results:
                    return {
                        'status': 'error',
                        'error': 'No files processed successfully',
                        'region': region_id,
                        'dataset': dataset
                    }
                
                return {
                    'status': 'success',
                    'processed_files': len(results),
                    'results': results,
                    'region': region_id,
                    'dataset': dataset
                }
            
            else:
                # Handle other data sources as before
                netcdf_path = await self._get_data(date, dataset, region_id, data_path)
                if not netcdf_path:
                    return {
                        'status': 'error',
                        'error': 'No data downloaded',
                        'region': region_id,
                        'dataset': dataset
                    }
                
                # Process the data
                logger.info("   └── 🔧 Processing data...")
                return await self._process_netcdf_data(
                    netcdf_path, region_id, dataset, date
                )
                
        except Exception as e:
            logger.error(f"Error processing {dataset} for {region_id}")
            logger.error(f"Error: {str(e)}")
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

    async def _get_data(self, date: datetime, dataset: str, region_id: str, cache_path: Path) -> Path:
        """Get data from cache or download"""
        logger.info(f"   └── Checking for cached data at {cache_path}")
        if cache_path.exists():
            logger.info(f"   └── ✅ Using cached data: {cache_path.name}")
            return cache_path

        logger.info(f"   └── 💾 No cache found, downloading from {SOURCES[dataset].get('source_type')}")
        source_type = SOURCES[dataset].get('source_type')
        try:
            if source_type == 'cmems':
                return await self.cmems_service.save_data(date, dataset, region_id)
            elif source_type == 'erddap':
                return await self.erddap_service.save_data(date, dataset, region_id)
            elif source_type == 'podaac':
                return await self.podaac_service.save_data(date, dataset, region_id)
            else:
                raise ProcessingError("download", f"Unknown source type: {source_type}",
                                   {"dataset": dataset, "region": region_id})
                
        except asyncio.CancelledError:
            logger.warning(f"Task cancelled for {dataset}")
            raise
        except Exception as e:
            raise ProcessingError("download", str(e), 
                                {"dataset": dataset, "region": region_id}) from e

    async def _process_netcdf_data(self, netcdf_path: Path, region_id: str, dataset: str, date: datetime):
        """Process the downloaded netCDF data."""
        try:
            # Get asset paths and dataset type
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)
            dataset_type = SOURCES[dataset]['type']
            
            # 1. Load and process data once
            logger.info(f"   └── Processing {dataset} data")
            with self.managed_netcdf(netcdf_path) as ds:
                var_name = SOURCES[dataset]['variables'][0]
                raw_data = ds[var_name]
                processed_data = self.data_preprocessor.preprocess_dataset(raw_data, dataset, region_id)
            
            # 2. Generate assets using processed data
            # Base GeoJSON
            logger.info(f"   └── Generating GeoJSON")
            geojson_converter = self.geojson_converter_factory.create(dataset, 'data')
            data_path = geojson_converter.convert(
                data=processed_data,
                region=region_id,
                dataset=dataset,
                date=date
            )

            # Contours for supported types
            if dataset_type in ['sst', 'chlorophyll']:
                logger.info(f"   └── Generating contours")
                contour_converter = self.geojson_converter_factory.create(dataset, 'contour')
                contour_path = contour_converter.convert(
                    data=processed_data,
                    region=region_id,
                    dataset=dataset,
                    date=date
                )

            # Image
            logger.info(f"   └── Generating image")
            processor = self.processor_factory.create(dataset_type)
            image_path, additional_layers = processor.generate_image(
                data=processed_data,
                region=region_id,
                dataset=dataset,
                date=date
            )

            # 3. Update metadata with processed data
            self.metadata_assembler.assemble_metadata(
                date=date,
                dataset=dataset,
                region=region_id,
                asset_paths=asset_paths,
                data=processed_data
            )

            logger.info("   └── ✅ Processing completed")
            return {
                'status': 'success',
                'paths': asset_paths._asdict(),
                'region': region_id,
                'dataset': dataset
            }

        except Exception as e:
            logger.error(f"Error processing {dataset} for {region_id}")
            logger.error(f"Error: {str(e)}")
            raise

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
