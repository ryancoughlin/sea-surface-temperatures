// Import the REGIONS object
// Define REGIONS object directly in this file
const REGIONS = {
  capecod: {
    coordinates: {
        minLat: 39.5,
        maxLat: 43.5,
        minLon: -71.25,
        maxLon: -65.25
    }
  }
  // Add other regions as needed
};

// Replace with your actual Mapbox token
const MAPBOX_TOKEN = 'pk.eyJ1Ijoic25vd2Nhc3QiLCJhIjoiY2plYXNjdTRoMDhsbDJ4bGFjOWN0YjdzeCJ9.fM2s4NZq_LUiTXJxsl2HbQ';

mapboxgl.accessToken = MAPBOX_TOKEN;

// Get Cape Cod region coordinates
const capeCodRegion = REGIONS.capecod.coordinates;

const map = new mapboxgl.Map({
    container: 'map',
    style: 'mapbox://styles/mapbox/dark-v10',
    center: [(capeCodRegion.minLon + capeCodRegion.maxLon) / 2, 
             (capeCodRegion.minLat + capeCodRegion.maxLat) / 2],
    zoom: 6
});

map.on('load', () => {
    // Add a single raster image source
    map.addSource('sst-raster', {
        type: 'image',
        url: 'sst.png', // Local tile image file
        coordinates: [
            [capeCodRegion.minLon, capeCodRegion.maxLat],
            [capeCodRegion.maxLon, capeCodRegion.maxLat],
            [capeCodRegion.maxLon, capeCodRegion.minLat],
            [capeCodRegion.minLon, capeCodRegion.minLat]
        ]
    });

    // Add a raster layer
    map.addLayer({
        id: 'sst-layer',
        type: 'raster',
        source: 'sst-raster',
        paint: {
            'raster-opacity': 0.8
        }
    });

    // Add a color legend
    const legend = document.createElement('div');
    legend.id = 'legend';
    legend.style.position = 'absolute';
    legend.style.bottom = '30px';
    legend.style.right = '10px';
    legend.style.padding = '10px';
    legend.style.background = 'rgba(255, 255, 255, 0.8)';
    legend.style.borderRadius = '5px';
    legend.innerHTML = `
        <h3>Temperature (°F)</h3>
        <div style="display: flex; align-items: center;">
            <div style="background: linear-gradient(to right, blue, cyan, green, yellow, red); width: 200px; height: 20px;"></div>
            <div style="display: flex; justify-content: space-between; width: 200px;">
                <span>32</span>
                <span>59</span>
                <span>86</span>
            </div>
        </div>
    `;
    document.body.appendChild(legend);

    // Add click event for temperature display
    map.on('click', 'sst-layer', (e) => {
        // Note: This will require your server to return temperature data for clicked coordinates
        fetch(`/api/sst/capecod/temperature?lon=${e.lngLat.lng}&lat=${e.lngLat.lat}`)
            .then(response => response.json())
            .then(data => {
                new mapboxgl.Popup()
                    .setLngLat(e.lngLat)
                    .setHTML(`Temperature: ${data.temperature.toFixed(1)}°F`)
                    .addTo(map);
            })
            .catch(error => console.error('Error fetching temperature data:', error));
    });
});
