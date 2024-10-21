mapboxgl.accessToken = 'pk.eyJ1Ijoic25vd2Nhc3QiLCJhIjoiY2plYXNjdTRoMDhsbDJ4bGFjOWN0YjdzeCJ9.fM2s4NZq_LUiTXJxsl2HbQ';

document.addEventListener('DOMContentLoaded', initMap);

function initMap() {
    const map = new mapboxgl.Map({
        container: 'map',
        style: 'mapbox://styles/mapbox/streets-v11',
        bounds: [
            [-72.22598321024519, 40.148157450000284],
            [-68.62114278975481, 43.401843566726924]
        ],
        minZoom: 5,
        maxZoom: 10
    });

    map.on('load', () => {
        addControls(map);
        setupImageOverlay(map);
        setupGeoJSONLayer(map);
        setupTemperatureDisplay(map);
        addRegionLayers(map);
    });
}

function addControls(map) {
    map.addControl(new mapboxgl.ScaleControl({
        maxWidth: 80,
        unit: 'imperial'
    }), 'bottom-right');

    map.addControl(new mapboxgl.NavigationControl({
        showZoom: true
    }), 'bottom-right');
}

function setupImageOverlay(map) {
    map.on('zoomend', () => updateImageOverlay(map));
    updateImageOverlay(map);
}

function updateImageOverlay(map) {
    const zoom = map.getZoom();
    const imageUrl = getImageUrlForZoom(zoom);

    if (map.getSource('sst-image')) {
        map.removeLayer('sst-layer');
        map.removeSource('sst-image');
    }

    if (imageUrl) {
        map.addSource('sst-image', {
            type: 'image',
            url: imageUrl,
            coordinates: [
                [-72.22598321024519, 43.401843566726924],
                [-68.62114278975481, 43.401843566726924],
                [-68.62114278975481, 40.148157450000284],
                [-72.22598321024519, 40.148157450000284]
            ]
        });

        map.addLayer({
            id: 'sst-layer',
            type: 'raster',
            source: 'sst-image',
            paint: {
                'raster-opacity': 0.7
            }
        });
    }
}

function getImageUrlForZoom(zoom) {
    if (zoom >= 5 && zoom < 8) return './capecod_sst_5.png';
    if (zoom >= 8 && zoom < 10) return './capecod_sst_8.png';
    if (zoom >= 10) return './capecod_sst_10.png';
    return null;
}

function setupGeoJSONLayer(map) {
    map.addSource('sst-data', {
        type: 'geojson',
        data: './sst_data.geojson'
    });

    map.addLayer({
        id: 'sst-points',
        type: 'circle',
        source: 'sst-data',
        paint: {
            'circle-radius': 5,
            'circle-opacity': 0,
            'circle-stroke-width': 1,
            'circle-stroke-opacity': 0
        }
    });
}

function setupTemperatureDisplay(map) {
    const tempInfo = document.getElementById('temp-info');

    map.on('mousemove', 'sst-points', (e) => {
        const features = map.queryRenderedFeatures(e.point, { layers: ['sst-points'] });
        if (!features.length) {
            tempInfo.textContent = 'Temperature: --°F';
            return;
        }
        const feature = features[0];
        tempInfo.textContent = `Temperature: ${feature.properties.sst_F.toFixed(2)}°F`;
    });

    map.on('mouseleave', 'sst-points', () => {
        tempInfo.textContent = 'Temperature: --°F';
    });
}

function addRegionLayers(map) {
    Object.values(REGIONS).forEach(region => {
        const { slug, name, coordinates } = region;
        const { minLon, minLat, maxLon, maxLat } = coordinates;

        // Add border layer
        map.addLayer({
            id: `${slug}-border`,
            type: 'line',
            source: {
                type: 'geojson',
                data: {
                    type: 'Feature',
                    geometry: {
                        type: 'Polygon',
                        coordinates: [[
                            [minLon, minLat],
                            [maxLon, minLat],
                            [maxLon, maxLat],
                            [minLon, maxLat],
                            [minLon, minLat]
                        ]]
                    }
                }
            },
            paint: {
                'line-color': '#888',
                'line-width': 2
            }
        });

        // Add text label
        map.addLayer({
            id: `${slug}-label`,
            type: 'symbol',
            source: {
                type: 'geojson',
                data: {
                    type: 'Feature',
                    geometry: {
                        type: 'Point',
                        coordinates: [(minLon + maxLon) / 2, (minLat + maxLat) / 2]
                    },
                    properties: {
                        name: name
                    }
                }
            },
            layout: {
                'text-field': ['get', 'name'],
                'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
                'text-size': 12,
                'text-anchor': 'center',
                'text-allow-overlap': true
            },
            paint: {
                'text-color': '#333',
                'text-halo-color': '#fff',
                'text-halo-width': 2
            }
        });
    });
}
