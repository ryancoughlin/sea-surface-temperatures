// Remove the import statement
// import * as L from 'leaflet';

console.log('Map script loaded');

document.addEventListener('DOMContentLoaded', initMap);

function initMap() {
    console.log('Initializing map');
    
    // Check if Leaflet is available
    if (typeof L === 'undefined') {
        console.error('Leaflet is not loaded. Make sure to include Leaflet library in your HTML.');
        return;
    }

    const map = L.map('map').setView([39, -75], 5);

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map);

    const regionSelect = document.getElementById('regionSelect');
    const dateSelect = document.getElementById('dateSelect');
    const captureDate = document.getElementById('captureDate');
    const tempDisplay = document.getElementById('tempDisplay');
    let deckOverlay = null;

    let cachedData = {};

    function getPastFiveDays() {
        const dates = [];
        for (let i = 0; i < 5; i++) {
            const date = new Date();
            date.setDate(date.getDate() - i);
            dates.push(date.toISOString().split('T')[0]);
        }
        console.log('Generated dates:', dates);  // Log the generated dates
        return dates;
    }

    async function fetchRegions() {
        if (cachedData.regions) return cachedData.regions;

        try {
            const response = await fetch('/api/regions');
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const regions = await response.json();
            cachedData.regions = regions;
            return regions;
        } catch (error) {
            console.error('Error fetching regions:', error);
            throw error;
        }
    }

    async function fetchSSTData(region, date) {
        const cacheKey = `${region}:${date}`;
        if (cachedData[cacheKey]) return cachedData[cacheKey];

        try {
            const url = `/api/sst/${region}/${date}`;
            console.log('Fetching SST data from:', url);
            const response = await fetch(url);
            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`HTTP error! status: ${response.status}, message: ${errorText}`);
            }
            const data = await response.json();
            console.log('Fetched SST data:', data);
            if (!data || typeof data !== 'object') throw new Error('Invalid data format received');
            cachedData[cacheKey] = data;
            return data;
        } catch (error) {
            console.error('Error fetching SST data:', error);
            throw error;
        }
    }

    function formatDate(dateString) {
        if (!dateString) return 'N/A';
        const date = new Date(dateString);
        return date.toISOString().split('T')[0];
    }

    function updateMap(data) {
        console.log('Updating map with data:', data)
        if (!data || typeof data !== 'object') {
            console.error('Invalid data format in updateMap')
            return
        }

        if (deckOverlay) {
            map.removeLayer(deckOverlay)
        }

        const sstData = data.features.map(f => ({
            position: f.geometry.coordinates,
            temperature: f.properties.temperature
        }))

        const deckLayer = new deck.HexagonLayer({
            id: 'hexagon-layer',
            data: sstData,
            pickable: true,
            extruded: false,
            radius: 20000,
            elevationScale: 1,
            getPosition: d => d.position,
            getElevationWeight: d => d.temperature,
            colorRange: [
                [33, 102, 172],
                [103, 169, 207],
                [209, 229, 240],
                [253, 219, 199],
                [239, 138, 98],
                [178, 24, 43]
            ],
            colorAggregation: 'MEAN',
            coverage: 1,
        })

        deckOverlay = L.deckGL({
            layers: [deckLayer],
            getTooltip: ({object}) => {
                if (!object) return null
                const temp = object.colorValue
                return {
                    html: `<div>Temperature: ${temp.toFixed(2)}°F</div>`,
                    style: {
                        backgroundColor: 'white',
                        fontSize: '0.8em',
                        padding: '4px'
                    }
                }
            }
        }).addTo(map)

        if (data.features && data.features.length > 0) {
            const bounds = L.geoJSON(data).getBounds()
            map.fitBounds(bounds)
        } else {
            console.warn('No features found in data')
        }
        
        if (data.properties && data.properties.captureDate) {
            captureDate.textContent = `Data captured on: ${formatDate(data.properties.captureDate)}`
        } else {
            captureDate.textContent = 'Capture date not available'
            console.warn('No capture date found in data')
        }
    }

    function populateRegions(regions) {
        regionSelect.innerHTML = '<option value="">Select a region</option>';
        regions.forEach(region => {
            const option = document.createElement('option');
            option.value = region.slug;
            option.textContent = region.name;
            regionSelect.appendChild(option);
        });
    }

    function populateDates() {
        const dates = getPastFiveDays();
        dateSelect.innerHTML = '<option value="">Select a date</option>';
        dates.forEach(date => {
            const option = document.createElement('option');
            option.value = date;
            option.textContent = date;
            dateSelect.appendChild(option);
        });
    }

    async function updateMapData() {
        const selectedRegion = regionSelect.value;
        const selectedDate = dateSelect.value;

        if (!selectedRegion || !selectedDate) {
            console.log('Region or date not selected');
            return;
        }

        try {
            const data = await fetchSSTData(selectedRegion, selectedDate);
            updateMap(data);
        } catch (error) {
            console.error('Error updating map data:', error);
            alert('Failed to fetch SST data. Please try again.');
        }
    }

    if (!regionSelect || !dateSelect || !captureDate || !tempDisplay) {
        console.error('One or more required DOM elements are missing');
        return;
    }

    // Populate regions
    fetchRegions()
        .then(regions => {
            populateRegions(regions);
            populateDates();
        })
        .catch(error => {
            console.error('Error initializing map:', error);
            alert('Failed to initialize map. Please refresh the page.');
        });

    regionSelect.addEventListener('change', updateMapData);
    dateSelect.addEventListener('change', updateMapData);

    map.on('mousemove', (e) => {
        const latlng = e.latlng;
        tempDisplay.innerHTML = `Lat: ${latlng.lat.toFixed(4)}, Lon: ${latlng.lng.toFixed(4)}`;
        tempDisplay.style.display = 'block';

        // Get temperature from TIF layer
        if (deckOverlay) {
            const temp = deckOverlay.getValueAtLatLng(latlng.lat, latlng.lng);
            if (temp !== null && temp !== undefined) {
                tempDisplay.innerHTML += `<br>Temperature: ${temp.toFixed(2)}°F`;
            }
        }
    });

    // Always show lat/lon, even when mouse leaves the map
    map.on('mouseout', () => {
        const center = map.getCenter();
        tempDisplay.innerHTML = `Lat: ${center.lat.toFixed(4)}, Lon: ${center.lng.toFixed(4)}`;
    });
}
