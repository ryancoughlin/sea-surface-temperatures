# SST Processor

Sea Surface Temperature Data Processing Pipeline

## Setup

1. Clone the repository
2. Install dependencies: `poetry install`
3. Copy `.env.example` to `.env` and configure
4. Run tests: `poetry run pytest`

## Usage

python
from src.data.processors.sst import SSTProcessor
processor = SSTProcessor()
await processor.process_latest()

## Project Structure

- `src/`: Source code
  - `config/`: Configuration management
  - `data/`: Data processing
  - `storage/`: Storage operations
  - `tiles/`: Tile generation
  - `utils/`: Utilities
- `tests/`: Test suite
