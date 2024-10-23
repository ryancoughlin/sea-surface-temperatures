import asyncio
from pathlib import Path
from src.processors.sst_processor import SSTProcessor
from src.config.settings import settings

async def main():
    processor = SSTProcessor()
    
    try:
        results = await processor.process_latest()
        for source_region, paths in results.items():
            print(f"Generated for {source_region}:")
            for zoom, full_image_path in paths['region'].items():
                print(f"- Region image (zoom {zoom}): {full_image_path}")
            for zoom, tile_paths in paths['tiles'].items():
                print(f"- Tiles (zoom {zoom}): {len(tile_paths)} files")
            
    except Exception as e:
        print(f"Error processing SST data: {e}")

if __name__ == "__main__":
    asyncio.run(main())
