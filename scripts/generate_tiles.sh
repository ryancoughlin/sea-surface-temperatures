#!/bin/bash

# Exit on error
set -e

# Check for required commands
command -v python3 >/dev/null 2>&1 || { echo "Python 3 is required but not installed. Aborting." >&2; exit 1; }
command -v tippecanoe >/dev/null 2>&1 || { echo "tippecanoe is required but not installed. Aborting." >&2; exit 1; }
command -v mb-util >/dev/null 2>&1 || { echo "mb-util is required but not installed. Please run: pip install mbutil. Aborting." >&2; exit 1; }

# Install required Python packages if not already installed
pip3 install geopandas fiona

# Run the Python script
echo "Generating vector tiles..."
python3 scripts/generate_vectortiles.py

echo "Process completed successfully!" 