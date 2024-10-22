import sys
import requests
import numpy as np
import matplotlib.pyplot as plt
from netCDF4 import Dataset
from io import BytesIO
from datetime import datetime

DATA_SOURCES = {
    "east_coast": {
        "base_url": "https://eastcoast.coastwatch.noaa.gov/data",
        "products": [
            {
                "name": "avhrr-viirs",
                "time_ranges": ["daily", "3day", "7day", "monthly"],
                "regions": ["gm", "cd", "ec", "ne", "ma", "sa"],
                "resolution": "750M"
            }
            # Add other products here
        ]
    }
}

def fetch_nc4_file(source, satellite, region, time_range, date):
    config = DATA_SOURCES[source]
    base_url = config["base_url"]
    product_config = next((p for p in config["products"] if p["name"] == satellite), None)
    
    if not product_config:
        raise ValueError(f"Invalid product: {satellite}")
    
    if time_range not in product_config["time_ranges"]:
        raise ValueError(f"Invalid time range for product {satellite}: {time_range}")
    
    if region not in product_config["regions"]:
        raise ValueError(f"Invalid region: {region}")

    # Convert date string to datetime object
    date_obj = datetime.strptime(date, "%Y%m%d")
    
    # Format the date part of the filename
    date_part = date_obj.strftime("%Y%j")  # YYYYDDD format

    # Construct the filename
    filename = f"ACSPOCW_{date_part}_{time_range.upper()}_MULTISAT_SST-NGT_{region.upper()}_{product_config['resolution']}.nc4"

    # Construct the full URL
    url = f"{base_url}/{satellite}/sst-ngt/{time_range}/{region}/{filename}"

    # Fetch the .nc4 file
    response = requests.get(url)
    if response.status_code == 200:
        return BytesIO(response.content)
    else:
        raise Exception(f"Failed to fetch .nc4 file: {response.status_code}")

def process_sst_data(nc4_data, source, satellite, region, time_range, date):
    with Dataset("temp.nc", "r", memory=nc4_data.getvalue()) as nc:
        sst = nc.variables['sst'][:]
        lats = nc.variables['lat'][:]
        lons = nc.variables['lon'][:]

    # Create a simple plot
    plt.figure(figsize=(10, 8))
    plt.imshow(sst, cmap='viridis', extent=[lons.min(), lons.max(), lats.min(), lats.max()])
    plt.colorbar(label='Sea Surface Temperature (°C)')
    plt.title(f'SST for {region.upper()} - {date}')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')

    # Save plot to BytesIO object
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png')
    img_buffer.seek(0)
    
    return img_buffer

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python process_sst.py <source> <satellite> <region> <time_range> <date>")
        sys.exit(1)

    source, satellite, region, time_range, date = sys.argv[1:]

    try:
        nc4_data = fetch_nc4_file(source, satellite, region, time_range, date)
        image_data = process_sst_data(nc4_data, source, satellite, region, time_range, date)
        sys.stdout.buffer.write(image_data.getvalue())
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)
