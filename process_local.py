from pathlib import Path
from src.data.processor import SSTProcessor
from src.config.settings import Settings, Region
from src.config.sources import SOURCES
from datetime import datetime

def main():
    settings = Settings()
    output_dir = Path("data/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize processor
    processor = SSTProcessor()
    
    # Get today's date in YYYYDDD format
    today = datetime.now().strftime("%Y%j")
    
    # Process each source and region
    for source_name, source_config in SOURCES.items():
        print(f"\nProcessing source: {source_name}")
        
        # Create source directory in raw path
        source_dir = settings.RAW_PATH / source_name
        source_dir.mkdir(parents=True, exist_ok=True)
        
        for region in source_config.regions:
            try:
                # Get file for this source/region with today's date
                input_file = settings.get_file_path(
                    source=source_name,
                    satellite=list(source_config.satellites.keys())[0],
                    date=today,
                    time_range="daily",
                    region=region
                )
                
                if not input_file.exists():
                    print(f"No data for {region.value} in {source_name}")
                    continue

                # Create region output directory
                region_dir = output_dir / source_name / region.value
                region_dir.mkdir(parents=True, exist_ok=True)

                print(f"Processing region: {region.value}")
                sst, lat, lon = processor.load_sst_data(input_file)
                processor.process_zoom_levels(sst, lat, lon, region_dir)
                
            except Exception as e:
                print(f"Error processing {region.value} for {source_name}: {e}")

if __name__ == "__main__":
    main()
