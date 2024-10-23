from pathlib import Path
from src.data.processor import SSTProcessor

def main():
    # Setup paths
    input_file = Path("../data/eastcoast.nc4")  # Your NC4 file path
    output_dir = Path("../data/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize processor
    processor = SSTProcessor()
    
    try:
        # Load and process data
        print(f"Processing file: {input_file}")
        sst, lat, lon = processor.load_sst_data(input_file)
        
        # Generate images for all zoom levels
        processor.process_zoom_levels(sst, lat, lon, output_dir)
        print("Processing completed successfully")
        
    except Exception as e:
        print(f"Error processing SST data: {e}")

if __name__ == "__main__":
    main()
