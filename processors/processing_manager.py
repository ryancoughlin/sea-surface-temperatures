from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from contextlib import contextmanager
import aiohttp
import logging
import asyncio
import xarray as xr

from services.erddap_service import ERDDAPService
from services.cmems_service import CMEMSService
from processors.metadata_assembler import MetadataAssembler
from processors.geojson.factory import GeoJSONConverterFactory
from processors.processor_factory import ProcessorFactory
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
        
        # Initialize factories once
        self.processor_factory = ProcessorFactory(path_manager)
        self.geojson_converter_factory = GeoJSONConverterFactory(path_manager)
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
            self.logger.info("📂 Loading dataset")
            ds = xr.open_dataset(
                path,
                chunks=None,
                decode_times=True
            )
            yield ds
        except Exception as e:
            self.logger.error("❌ Error loading dataset")
            self.logger.error(f"   └── 💥 {str(e)}")
            raise ProcessingError("loading", str(e), {"path": str(path)})
        finally:
            if ds:
                ds.close()
            if cleanup and path.exists():
                path.unlink()

    async def process_dataset(self, date: datetime, region_id: str, dataset: str) -> Dict:
        """Process a single dataset with proper async handling"""
        if not self.session:
            raise ProcessingError("initialization", "ProcessingManager not initialized", 
                                {"dataset": dataset, "region": region_id})
        
        logger.info(f"📦 Processing dataset:")
        logger.info(f"   └── Dataset: {dataset}")
        logger.info(f"   └── Region: {region_id}")
        logger.info(f"   └── Date: {date}")

        try:
            # Get paths
            data_path = self.path_manager.get_data_path(date, dataset, region_id)
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)

            # Use cached or download new data
            logger.info("   └── 🔄 Fetching data...")
            netcdf_path = await self._get_data(date, dataset, region_id, data_path)
            
            # Process the data
            logger.info("   └── 🔧 Processing data...")
            result = await self._process_netcdf_data(
                netcdf_path, region_id, dataset, date
            )
            
            if result['status'] == 'success':
                logger.info("   └── ✅ Processing complete")
            return result

        except asyncio.TimeoutError:
            logger.error(f"Timeout while downloading data for {dataset}")
            return {
                'status': 'error',
                'error': 'Download timeout',
                'region': region_id,
                'dataset': dataset
            }
        except Exception as e:
            logger.error("   └── 💥 Processing failed")
            return self._handle_error(e, dataset, region_id)

    async def _get_data(self, date: datetime, dataset: str, region_id: str, cache_path: Path) -> Path:
        """Get data from cache or download"""
        if cache_path.exists():
            logger.info(f"Using cached data: {cache_path.name}")
            return cache_path

        source_type = SOURCES[dataset].get('source_type')
        try:
            if source_type == 'cmems':
                return await self.cmems_service.save_data(date, dataset, region_id)
            elif source_type == 'erddap':
                return await self.erddap_service.save_data(date, dataset, region_id)
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
            # Get asset paths first
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)
            
            # Get dataset type early
            dataset_type = SOURCES[dataset]['type']
            
            # Load data
            ds = xr.open_dataset(netcdf_path)
            var_name = SOURCES[dataset]['variables'][0]
            data = ds[var_name]
            
            # Create interpolated path
            interpolated_path = netcdf_path.parent / f"{netcdf_path.stem}_interpolated.nc"
            
            # Only apply land masking to chlorophyll data
            if dataset_type == 'chlorophyll':
                logger.info(f"Masking land for chlorophyll: {dataset}")
                data = self.land_masker.mask_land(data)
                
                # Save masked data to new netCDF file
                masked_path = netcdf_path.parent / f"{netcdf_path.stem}_masked.nc"
                data.to_netcdf(masked_path)
                netcdf_path = masked_path
            
            try:
                # Generate base GeoJSON
                geojson_converter = self.geojson_converter_factory.create(dataset, 'data')
                data_path = geojson_converter.convert(
                    data_path=netcdf_path,
                    region=region_id,
                    dataset=dataset,
                    date=date
                )

                # Generate contours if supported
                if dataset_type in ['sst', 'chlorophyll']:
                    self.logger.info(f"Generating {dataset_type} contours for {dataset}")
                    contour_converter = self.geojson_converter_factory.create(dataset, 'contour')
                    contour_path = contour_converter.convert(
                        data_path=netcdf_path,
                        region=region_id,
                        dataset=dataset,
                        date=date
                    )

                # Generate image
                processor = self.processor_factory.create(dataset_type)
                self.logger.info(f"Processing {dataset} data for {region_id}")
                image_path = processor.generate_image(
                    data_path=netcdf_path,
                    region=region_id,
                    dataset=dataset,
                    date=date
                )

                # Update metadata after all assets are generated
                self.metadata_assembler.assemble_metadata(
                    date=date,
                    dataset=dataset,
                    region=region_id,
                    asset_paths=asset_paths
                )

                self.logger.info("✅ Processing completed")

                return {
                    'status': 'success',
                    'paths': asset_paths._asdict(),
                    'region': region_id,
                    'dataset': dataset
                }

            finally:
                # Clean up interpolated file if it exists
                if interpolated_path.exists():
                    interpolated_path.unlink()

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
