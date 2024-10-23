from pathlib import Path
from src.data.processor import SSTProcessor
from src.config.settings import settings

def main():
    # Setup paths
    input_file = Path("data/eastcoast.nc4")  # Update this path
    output_dir = settings.TILE_PATH
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize processor
    processor = SSTProcessor()
    
    try:
        # Load and process data
        print(f"Processing file: {input_file}")
        sst, lat, lon = processor.load_sst_data(input_file)
        
        # Generate images for zoom level 5 (simplest case)
        paths = processor.tile_generator.generate_tiles(sst, lat, lon, 5, output_dir)
        print(f"Generated tiles: {paths}")
        
    except Exception as e:
        print(f"Error processing SST data: {e}")

if __name__ == "__main__":
    main()
