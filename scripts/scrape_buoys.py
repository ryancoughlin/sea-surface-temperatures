import requests
from bs4 import BeautifulSoup
import json

# Step 1: Fetch the Page
url = "https://www.ndbc.noaa.gov/to_station.shtml"
response = requests.get(url)

if response.status_code != 200:
    print("Failed to fetch the webpage.")
    exit()

soup = BeautifulSoup(response.text, "html.parser")

# Step 2: Parse the Station List
stations_div = soup.find("div", {"class": "station-list"})
if not stations_div:
    print("Station list not found. Please check the markup.")
    exit()

stations = []

# Extract station data from the links
for link in stations_div.find_all("a"):
    href = link.get("href")
    station_id = link.get_text(strip=True)
    if href and "station_page.php" in href:
        stations.append({
            "station_id": station_id,
            "href": href  # Include href for potential future use
        })

# Step 3: Generate GeoJSON Skeleton
geojson = {
    "type": "FeatureCollection",
    "features": []
}

# Step 4: Add Coordinates (Placeholder - No lat/lon in provided markup)
for station in stations:
    # Here, you may need to fetch lat/lon from the station's page or another source
    geojson["features"].append({
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [0, 0]  # Replace with actual lat/lon if available
        },
        "properties": {
            "station_id": station["station_id"],
            "href": station["href"]
        }
    })

# Step 5: Save GeoJSON File
output_file = "ndbc_stations_placeholder.geojson"
with open(output_file, "w") as f:
    json.dump(geojson, f, indent=2)

print(f"GeoJSON file saved: {output_file}")