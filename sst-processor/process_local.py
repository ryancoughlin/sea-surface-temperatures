import asyncio
from pathlib import Path
from src.data.processors.sst_processor import SSTProcessor
from src.config.settings import settings

async def main():
    processor = SSTProcessor()
    
    try:
        # Process all configured sources
        results = await processor.process_latest()
        for source_region, paths in results.items():
            print(f"Generated tiles for {source_region}: {len(paths)} files")
            
    except Exception as e:
        print(f"Error processing SST data: {e}")

if __name__ == "__main__":
    asyncio.run(main())
