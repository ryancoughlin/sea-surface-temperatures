import copernicusmarine
from datetime import datetime, timedelta
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | 🔄 %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def test_cmems_download():
    # Test parameters
    dataset_id = "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i"
    variables = ["VHM0", "VMDR", "VTM10", "VTPK", "VPED"]
    
    # Gulf of Maine bounds (example)
    bounds = [
        [-71.0, 41.0],  # [min_lon, min_lat]
        [-68.0, 44.0]   # [max_lon, max_lat]
    ]
    
    # Date range (today - 1 day)
    date = datetime.now() - timedelta(days=1)
    
    # Output path
    output_dir = Path("test_output")
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / f"cmems_waves_{date.strftime('%Y%m%d')}.nc"
    
    try:
        # 1. Check dataset info
        logger.info("📊 Checking dataset info...")
        info = copernicusmarine.get_dataset_info(dataset_id)
        logger.info(f"   └── Status: {info.get('status', 'unknown')}")
        logger.info(f"   └── Variables available: {', '.join(info.get('variables', []))}")
        logger.info(f"   └── Time coverage: {info.get('time_coverage', 'unknown')}")
        
        # 2. Download data
        logger.info("\n📥 Starting download...")
        logger.info(f"   └── Dataset: {dataset_id}")
        logger.info(f"   └── Variables: {variables}")
        logger.info(f"   └── Region: {bounds}")
        logger.info(f"   └── Date: {date.strftime('%Y-%m-%d')}")
        logger.info(f"   └── Output: {output_path}")
        
        copernicusmarine.subset(
            dataset_id=dataset_id,
            variables=variables,
            minimum_longitude=bounds[0][0],
            maximum_longitude=bounds[1][0],
            minimum_latitude=bounds[0][1],
            maximum_latitude=bounds[1][1],
            start_datetime=date.strftime("%Y-%m-%dT00:00:00"),
            end_datetime=date.strftime("%Y-%m-%dT23:59:59"),
            output_filename=str(output_path),
            force_download=True
        )
    except Exception as e:
        logger.error(f"❌ Error during download: {e}")

test_cmems_download() 