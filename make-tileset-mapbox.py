import requests

# Set your Mapbox access token here
MAPBOX_ACCESS_TOKEN = 'sk.eyJ1Ijoic25vd2Nhc3QiLCJhIjoiY2x3Y25wOHU2MHpxbzJrbnZ4Z3p5b3V5cyJ9.rPG-EQTtKhU_6XKgvr6s1w'

# Define your Mapbox username and the desired tileset source ID and tileset name
USERNAME = 'snowcast'
TILESET_SOURCE_ID = 'sst_may_18'
TILESET_NAME = 'sst_us'

# Define the path to your line-delimited GeoJSON file
geojson_file_path = './us-sst.geojson'

def create_tileset_source(username, source_id, file_path):
	url = f"https://api.mapbox.com/tilesets/v1/sources/{username}/{source_id}?access_token={MAPBOX_ACCESS_TOKEN}"
	with open(file_path, 'rb') as file:
		files = {'file': file}
		response = requests.post(url, files=files, headers={"Content-Type": "multipart/form-data"})
	
	if response.status_code != 200:
		print(f"Error creating tileset source: {response.status_code} {response.text}")
		response.raise_for_status()
	
	return response.json()

def create_tileset(username, tileset_name, source_id):
	url = f"https://api.mapbox.com/tilesets/v1/{username}.{tileset_name}?access_token={MAPBOX_ACCESS_TOKEN}"
	payload = {
		"recipe": {
			"version": 1,
			"layers": {
				"layer0": {
					"source": f"mapbox://tileset-source/{username}/{source_id}",
					"minzoom": 0,
					"maxzoom": 5
				}
			}
		},
		"name": tileset_name,
		"description": "Tileset created from GeoJSON"
	}
	response = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
	
	if response.status_code != 200:
		print(f"Error creating tileset: {response.status_code} {response.text}")
		response.raise_for_status()
	
	return response.json()

def publish_tileset(username, tileset_name):
	url = f"https://api.mapbox.com/tilesets/v1/{username}.{tileset_name}/publish?access_token={MAPBOX_ACCESS_TOKEN}"
	response = requests.post(url)
	
	if response.status_code != 200:
		print(f"Error publishing tileset: {response.status_code} {response.text}")
		response.raise_for_status()
	
	return response.json()

def main():
	# Create tileset source
	tileset_source_response = create_tileset_source(USERNAME, TILESET_SOURCE_ID, geojson_file_path)
	print('Tileset source created successfully:', tileset_source_response)
	
	# Create tileset
	create_tileset_response = create_tileset(USERNAME, TILESET_NAME, TILESET_SOURCE_ID)
	print('Tileset created successfully:', create_tileset_response)
	
	# Publish tileset
	publish_response = publish_tileset(USERNAME, TILESET_NAME)
	print('Tileset published successfully:', publish_response)

if __name__ == '__main__':
	main()
