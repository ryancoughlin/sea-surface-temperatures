mapboxgl.accessToken = 'pk.eyJ1Ijoic25vd2Nhc3QiLCJhIjoiY2plYXNjdTRoMDhsbDJ4bGFjOWN0YjdzeCJ9.fM2s4NZq_LUiTXJxsl2HbQ';

document.addEventListener('DOMContentLoaded', initMap);

function initMap() {
    const map = new mapboxgl.Map({
        container: 'map',
        style: 'mapbox://styles/mapbox/streets-v11',
        center: [-72, 38], // Centered on the East Coast
        zoom: 5,
        minZoom: 5,
        maxZoom: 10
    });

    map.on('load', () => {
        addControls(map);
        addRegionLayers(map);
        setupImageOverlays(map);
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

function setupImageOverlays(map) {
    map.on('zoomend', () => updateImageOverlays(map));
    updateImageOverlays(map);
}

function updateImageOverlays(map) {
    const zoom = map.getZoom();
    
    Object.values(REGIONS).forEach(region => {
        const { slug, coordinates } = region;
        const { minLon, minLat, maxLon, maxLat } = coordinates;
        const imageUrl = getImageUrlForZoom(zoom, slug);

        const sourceId = `${slug}-sst-image`;
        const layerId = `${slug}-sst-layer`;

        if (map.getSource(sourceId)) {
            map.removeLayer(layerId);
            map.removeSource(sourceId);
        }

        if (imageUrl) {
            map.addSource(sourceId, {
                type: 'image',
                url: imageUrl,
                coordinates: [
                    [minLon, maxLat],
                    [maxLon, maxLat],
                    [maxLon, minLat],
                    [minLon, minLat]
                ]
            });

            map.addLayer({
                id: layerId,
                type: 'raster',
                source: sourceId,
                paint: {
                    'raster-opacity': 0.7
                }
            });
        }
    });
}

function getImageUrlForZoom(zoom, region) {
    if (zoom >= 5 && zoom < 8) return `./${region}_sst_zoom_5.png`;
    if (zoom >= 8 && zoom < 10) return `./${region}_sst_zoom_8.png`;
    if (zoom >= 10) return `./${region}_sst_zoom_10.png`;
    return null;
}
