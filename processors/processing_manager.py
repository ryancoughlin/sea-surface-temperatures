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
from utils.data_utils import interpolate_dataset
import xarray as xr

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
        self.logger = logging.getLogger(__name__)

    async def initialize(self, session: aiohttp.ClientSession):
        """Initialize services with session"""
        self.session = session
        self.erddap_service = ERDDAPService(session, self.path_manager)
        self.cmems_service = CMEMSService(session, self.path_manager)

    def _log_processing_summary(self, dataset: str, region: str, output: str):
        """Log processing summary with consistent formatting"""
        self.logger.info("📊 Processing Summary")
        self.logger.info(f"   ├── 📦 Dataset: {dataset}")
        self.logger.info(f"   ├── 🌎 Region:  {region}")
        self.logger.info(f"   └── 📄 Output:  {output or 'None'}")

    async def process_dataset(self, date: datetime, region_id: str, dataset: str) -> Dict:
        """Process a single dataset"""
        if not self.session:
            raise RuntimeError("ProcessingManager not initialized")
        
        try:
            # Log start of processing with clear structure
            self.logger.info(f"🔄 Processing {dataset}")
            self.logger.info(f"   ├── 🌎 Region: {region_id}")
            self.logger.info(f"   └── 📅 Date: {date.strftime('%Y-%m-%d')}")
            
            # Get paths
            data_path = self.path_manager.get_data_path(date, dataset, region_id)
            asset_paths = self.path_manager.get_asset_paths(date, dataset, region_id)

            # Use cached data if available
            if data_path.exists():
                self.logger.info(f"📂 Using cached data")
                return await self._process_netcdf_data(
                    data_path, region_id, dataset, date, asset_paths
                )

            # Determine service type and download data
            source_type = SOURCES[dataset].get('source_type')
            self.logger.info(f"Using service type: {source_type} for dataset: {dataset}")
            
            try:
                if source_type == 'cmems':
                    netcdf_path = await self.cmems_service.save_data(date, dataset, region_id)
                elif source_type == 'erddap':
                    netcdf_path = await self.erddap_service.save_data(date, dataset, region_id)
                else:
                    raise ValueError(f"Unknown source type: {source_type}")
                
                if not netcdf_path or not netcdf_path.exists():
                    raise FileNotFoundError(f"Failed to download data for {dataset}")
                
                self.logger.info(f"Successfully downloaded data to: {netcdf_path}")
                
                # Process the downloaded data
                return await self._process_netcdf_data(
                    netcdf_path, region_id, dataset, date, asset_paths
                )
                
            except asyncio.CancelledError:
                self.logger.warning(f"Task cancelled for dataset: {dataset}")
                raise
            except Exception as e:
                self.logger.error(f"Download error for {dataset}: {str(e)}")
                return {
                    'status': 'error',
                    'error': str(e),
                    'region': region_id,
                    'dataset': dataset
                }

        except Exception as e:
            self.logger.error(
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
            # Only load required variables instead of entire dataset
            required_vars = SOURCES[dataset].get('variables', [])
            type = SOURCES[dataset]['type']
      
            # Load and get dimensions dynamically
            with xr.open_dataset(netcdf_path) as ds:
                # Use the actual sizes from the dataset
                chunks = {
                    'time': 1,
                    'latitude': ds.sizes['latitude'],
                    'longitude': ds.sizes['longitude']
                }
                
                # Add depth chunk only if it exists
                if 'depth' in ds.sizes:
                    chunks['depth'] = 1
                
                ds = ds[required_vars].chunk(chunks)

                if type == 'sst':
                    interpolated_ds = interpolate_dataset(ds, 1)
                else:
                    interpolated_ds = interpolate_dataset(ds, 1.5)
                
                # Save interpolated dataset with compression
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
                
                # Generate base GeoJSON data using interpolated dataset
                geojson_converter = self.geojson_converter_factory.create(dataset, 'data')
                data_path = geojson_converter.convert(
                    data_path=interpolated_path,
                    region=region_id,
                    dataset=dataset,
                    date=date
                )

                # Generate contours for SST data
                if SOURCES[dataset]['type'] == 'sst':
                    contour_converter = self.geojson_converter_factory.create(dataset, 'contour')
                    contour_path = contour_converter.convert(
                        data_path=interpolated_path,
                        region=region_id,
                        dataset=dataset,
                        date=date
                    )

                # Generate image using dataset type
                try:
                    dataset_type = SOURCES[dataset]['type']
                    processor = self.processor_factory.create(dataset_type)
                    self.logger.info(f"Processing {dataset} data for {region_id}")
                    image_path = processor.generate_image(
                        data_path=interpolated_path,
                        region=region_id,
                        dataset=dataset,
                        date=date
                    )
                except ValueError as e:
                    self.logger.error(f"Processor error for type {dataset_type}: {str(e)}")
                    raise
                except KeyError as e:
                    self.logger.error(f"Missing type configuration for dataset {dataset}") 
                finally:
                    # Clean up interpolated file
                    if interpolated_path.exists():
                        interpolated_path.unlink()

                metadata_path: Path = self.metadata_assembler.assemble_metadata(
                    region=region_id,
                    dataset=dataset,
                    date=date,
                    asset_paths=asset_paths
                )

                self.logger.info("✅ Processing completed")
                self._log_processing_summary(dataset, region_id, str(data_path))

                return {
                    'status': 'success',
                    'paths': asset_paths._asdict(),
                    'region': region_id,
                    'dataset': dataset
                }
        except Exception as e:
            self.logger.error(f"Error in _process_netcdf_data: {str(e)}")
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