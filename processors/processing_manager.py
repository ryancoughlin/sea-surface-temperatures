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
from utils.data_utils import interpolate_dataset

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
        """Process a single dataset"""
        if not self.session:
            raise ProcessingError("initialization", "ProcessingManager not initialized", 
                                {"dataset": dataset, "region": region_id})
        
        try:
            logger.info(f"Processing {dataset} for {region_id}")
            
            # Get paths
            data_path = self.path_manager.get_data_path(date, dataset, region_id)
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)

            # Use cached or download new data
            netcdf_path = await self._get_data(date, dataset, region_id, data_path)
            
            # Process the data
            return await self._process_netcdf_data(
                netcdf_path, region_id, dataset, date, asset_paths
            )

        except Exception as e:
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

    async def _process_netcdf_data(self, netcdf_path: Path, region_id: str, dataset: str, date: datetime, asset_paths):
        """Process NetCDF data and generate outputs"""
        try:
            required_vars = SOURCES[dataset].get('variables', [])
            dataset_type = SOURCES[dataset]['type']
            
            # Original working pattern
            with xr.open_dataset(
                netcdf_path,
                chunks=None,  # Let xarray use the original file chunks
                decode_times=True
            ) as ds:
                ds = ds[required_vars]
                interpolated_ds = interpolate_dataset(ds)
                
                interpolated_path = netcdf_path.parent / f"{netcdf_path.stem}_interpolated.nc"
                interpolated_ds.to_netcdf(
                    interpolated_path,
                    encoding={
                        var: {
                            'zlib': True,
                            'complevel': 5,
                            '_FillValue': -9999.0
                        } for var in interpolated_ds.data_vars
                    }
                )

                try:
                    # Generate base GeoJSON
                    geojson_converter = self.geojson_converter_factory.create(dataset, 'data')
                    data_path = geojson_converter.convert(
                        data_path=interpolated_path,  # Use interpolated NetCDF
                        region=region_id,
                        dataset=dataset,
                        date=date
                    )

                    # Generate contours for SST
                    if dataset_type == 'sst':
                        contour_converter = self.geojson_converter_factory.create(dataset, 'contour')
                        contour_path = contour_converter.convert(
                            data_path=interpolated_path,  # Use interpolated NetCDF
                            region=region_id,
                            dataset=dataset,
                            date=date
                        )

                    # Generate image
                    processor = self.processor_factory.create(dataset_type)
                    image_path = processor.generate_image(
                        data_path=interpolated_path,  # Use interpolated NetCDF
                        region=region_id,
                        dataset=dataset,
                        date=date
                    )

                    return {
                        'status': 'success',
                        'paths': asset_paths._asdict(),
                        'region': region_id,
                        'dataset': dataset
                    }
                finally:
                    if interpolated_path.exists():
                        interpolated_path.unlink()

        except Exception as e:
            logger.error(f"Error processing {dataset} for {region_id}")
            logger.error(f"Error: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'region': region_id,
                'dataset': dataset
            }

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
