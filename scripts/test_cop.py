import copernicusmarine
from datetime import datetime

# Basic parameters
dataset_id = "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m"  # Global ocean currents
date = datetime.now().strftime("%Y-%m-%d")
output_file = "test_download.nc"

# Simple subset request
data = copernicusmarine.subset(
    dataset_id=dataset_id,
    variables=["uo", "vo"],  # East and North current components
    minimum_longitude=-98,
    maximum_longitude=-80,
    minimum_latitude=18,
    maximum_latitude=31,
    start_datetime=f"{date}T00:00:00",
    end_datetime=f"{date}T23:59:59",
    output_filename=output_file
)

print(f"Download complete: {output_file}")