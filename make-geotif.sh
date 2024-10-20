#!/bin/bash

# Check if a file path was provided
if [ $# -eq 0 ]; then
    echo "Please provide the path to the NetCDF file."
    echo "Usage: $0 /path/to/your/input_file.nc [resolution] [smoothing]"
    exit 1
fi

# Set default values
RESOLUTION=${2:-4}
SMOOTHING=${3:-2}

# Run the Python script
python /path/to/your/python_script.py "$1" --resolution "$RESOLUTION" --smoothing "$SMOOTHING"

# If the script ran successfully, try to open the resulting files
if [ $? -eq 0 ]; then
    OUTPUT_TIF="${1%.*}_interpolated.tif"
    OUTPUT_PNG="${1%.*}_interpolated_colormap.png"
    
    if [ -f "$OUTPUT_TIF" ]; then
        open "$OUTPUT_TIF"
    fi
    
    if [ -f "$OUTPUT_PNG" ]; then
        open "$OUTPUT_PNG"
    fi
else
    echo "An error occurred while processing the file."
fi