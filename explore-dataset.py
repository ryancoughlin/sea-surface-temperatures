import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Load the dataset and analyze the data structure
file_path = "./data/eastcoast.nc4"  # Replace with the correct path to your NetCDF file

# Open the dataset
try:
    ds = xr.open_dataset(file_path)
    print("Dataset loaded successfully.")
    
    # Overview of the dataset structure
    print("\n--- Dataset Overview ---")
    print(ds)
    
    # Extract and summarize all variables
    variables_summary = []
    for var_name, var_data in ds.data_vars.items():
        var_summary = {
            "Variable Name": var_name,
            "Dimensions": var_data.dims,
            "Shape": var_data.shape,
            "Attributes": var_data.attrs,
            "Min Value": float(var_data.min().values) if np.isscalar(var_data.min().values) else None,
            "Max Value": float(var_data.max().values) if np.isscalar(var_data.max().values) else None,
            "Mean Value": float(var_data.mean().values) if np.isscalar(var_data.mean().values) else None,
            "Standard Deviation": float(var_data.std().values) if np.isscalar(var_data.std().values) else None
        }
        variables_summary.append(var_summary)

    # Convert summary to a DataFrame for easy readability
    summary_df = pd.DataFrame(variables_summary)
    print("\n--- Variables Summary ---")
    print(summary_df)

    # Extract latitude, longitude, time, and SST data (if available)
    lat_data = ds.get('lat', None)
    lon_data = ds.get('lon', None)
    time_data = ds.get('time', None)
    sst_data = ds.get('sst', None)

    if lat_data is not None and lon_data is not None and time_data is not None and sst_data is not None:
        # Calculate the min and max SST in Fahrenheit
        sst_min_celsius = float(sst_data.min().values)
        sst_max_celsius = float(sst_data.max().values)
        sst_min_fahrenheit = (sst_min_celsius * 9 / 5) + 32
        sst_max_fahrenheit = (sst_max_celsius * 9 / 5) + 32

        # Calculate the number of data points and spacing
        num_lat_points = len(lat_data)
        num_lon_points = len(lon_data)
        num_time_points = len(time_data)

        # Calculate resolution of latitude and longitude in degrees
        if num_lat_points > 1:
            lat_resolution_deg = abs(float(lat_data.values[1] - lat_data.values[0]))
        else:
            lat_resolution_deg = None

        if num_lon_points > 1:
            lon_resolution_deg = abs(float(lon_data.values[1] - lon_data.values[0]))
        else:
            lon_resolution_deg = None

        # Calculate temporal resolution (time interval)
        if num_time_points > 1:
            time_resolution = np.abs((time_data.values[1] - time_data.values[0]) / np.timedelta64(1, 'h'))
        else:
            time_resolution = None

        # Calculate approximate resolution in miles (assuming 69 miles per degree of latitude)
        miles_per_degree = 69  # Approximate value for both latitude and longitude
        lat_resolution_miles = lat_resolution_deg * miles_per_degree if lat_resolution_deg else None
        lon_resolution_miles = lon_resolution_deg * miles_per_degree if lon_resolution_deg else None

        # Output results in a user-friendly way
        print("\n--- Latitude and Longitude Data ---")
        print(f"Number of Latitude Points: {num_lat_points}")
        print(f"Number of Longitude Points: {num_lon_points}")
        print(f"Number of Time Points: {num_time_points}")
        
        if lat_resolution_deg and lon_resolution_deg:
            print(f"Latitude Resolution: {lat_resolution_deg:.2f} degrees (~{lat_resolution_miles:.2f} miles)")
            print(f"Longitude Resolution: {lon_resolution_deg:.2f} degrees (~{lon_resolution_miles:.2f} miles)")
        else:
            print("Resolution data is not sufficient to calculate detailed spacing.")

        if time_resolution:
            print(f"Temporal Resolution: {time_resolution:.2f} hours")
        else:
            print("Temporal data is not sufficient to calculate time resolution.")
        
        print(f"\n--- Sea Surface Temperature (SST) ---")
        print(f"Minimum SST: {sst_min_fahrenheit:.2f} °F")
        print(f"Maximum SST: {sst_max_fahrenheit:.2f} °F")
        print(f"Mean SST: {((sst_data.mean().values * 9 / 5) + 32):.2f} °F")
        print(f"Standard Deviation of SST: {((sst_data.std().values * 9 / 5)):.2f} °F")

        # Plotting histogram of SST data for better understanding of distribution
        print("\n--- SST Data Distribution ---")
        sst_data_fahrenheit = (sst_data * 9 / 5) + 32
        sst_data_fahrenheit.plot.hist(bins=50, alpha=0.7)
        plt.xlabel("Sea Surface Temperature (°F)")
        plt.ylabel("Frequency")
        plt.title("Distribution of Sea Surface Temperature (SST)")
        plt.grid(True)
        plt.show()
    else:
        print("\nLatitude, Longitude, Time, or SST data not found in the dataset.")

    print("\nNote: SST is displayed in Fahrenheit for easier interpretation.")

    # Instructions for creating map tiles
    print("\n--- Instructions for Map Tiles ---")
    print("To create three different map tiles for detailed views:")
    print("1. Low Resolution: Aggregate data to reduce the number of points (e.g., average every 5 lat/lon points).")
    print("2. Medium Resolution: Use the raw lat/lon data for a general overview of the area.")
    print("3. High Resolution: Interpolate data to a finer grid to show detailed local variations.")

except Exception as e:
    print(f"Error loading the dataset: {e}")
