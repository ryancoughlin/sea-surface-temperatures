#!/bin/bash

# Variables
MAPBOX_ACCESS_TOKEN='sk.eyJ1Ijoic25vd2Nhc3QiLCJhIjoiY2x3Y25wOHU2MHpxbzJrbnZ4Z3p5b3V5cyJ9.rPG-EQTtKhU_6XKgvr6s1w'
USERNAME='snowcast'
TILESET_SOURCE_ID='sst_may_18'
TILESET_NAME='sst_us'
GEOJSON_FILE_PATH='./sst_data.geojson'

# Step 1: Create a tileset source
echo "Creating tileset source..."
curl -X POST "https://api.mapbox.com/tilesets/v1/sources/${USERNAME}/${TILESET_SOURCE_ID}?access_token=${MAPBOX_ACCESS_TOKEN}" \
	-F file=@${GEOJSON_FILE_PATH} \
	--header "Content-Type: multipart/form-data"

# Step 2: Create a tileset
echo "Creating tileset..."
curl -X POST "https://api.mapbox.com/tilesets/v1/${USERNAME}.${TILESET_NAME}?access_token=${MAPBOX_ACCESS_TOKEN}" \
	-H "Content-Type: application/json" \
	-d '{
		"recipe": {
			"version": 1,
			"layers": {
				"layer0": {
					"source": "mapbox://tileset-source/'${USERNAME}'/'${TILESET_SOURCE_ID}'",
					"minzoom": 0,
					"maxzoom": 5
				}
			}
		},
		"name": "'${TILESET_NAME}'",
		"description": "Tileset created from GeoJSON"
	}'

# Step 3: Publish the tileset
echo "Publishing tileset..."
curl -X POST "https://api.mapbox.com/tilesets/v1/${USERNAME}.${TILESET_NAME}/publish?access_token=${MAPBOX_ACCESS_TOKEN}"

echo "Tileset published successfully!"
