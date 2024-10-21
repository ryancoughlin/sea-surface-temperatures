// Remove the import statement
// import * as L from 'leaflet';

console.log('Map script loaded');

document.addEventListener('DOMContentLoaded', initMap);

function initMap() {
    console.log('Initializing map')
    
    if (typeof L === 'undefined') {
        console.error('Leaflet is not loaded. Make sure to include Leaflet library in your HTML.')
        return
    }

    const bounds = L.latLngBounds(
        L.latLng(40.15, -72.23),
        L.latLng(43.40, -68.62)
    )

    const map = L.map('map', {
        center: bounds.getCenter(),
        zoom: 7,
        minZoom: 7,
        maxZoom: 10,
        maxBounds: bounds,
        maxBoundsViscosity: 1.0
    })

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map)

    const images = {
        5: L.imageOverlay('capecod_sst_5.png', bounds),
        8: L.imageOverlay('capecod_sst_8.png', bounds),
        10: L.imageOverlay('capecod_sst_12.png', bounds)
    }

    const info = createInfoControl()
    info.addTo(map)

    function updateImage() {
        const zoom = map.getZoom()
        const imageToShow = getImageForZoom(zoom)

        map.eachLayer(layer => {
            if (layer instanceof L.ImageOverlay) map.removeLayer(layer)
        })
        if (imageToShow) imageToShow.addTo(map).setOpacity(0.7)

        info.update()
        updateMapInfo()
    }

    map.on('zoomend', updateImage)
    updateImage()
    map.fitBounds(bounds)
}

function createInfoControl() {
    const info = L.control()
    info.onAdd = function () {
        this._div = L.DomUtil.create('div', 'info')
        this.update()
        return this._div
    }
    info.update = function () {
        const zoom = map.getZoom()
        const activeTile = getActiveTile(zoom)
        this._div.innerHTML = `<h4>Map Info</h4><b>Zoom level:</b> ${zoom}<br><b>Active tile:</b> ${activeTile}`
    }
    return info
}

function getImageForZoom(zoom) {
    if (zoom >= 5 && zoom < 8) return images[5]
    if (zoom >= 8 && zoom < 10) return images[8]
    if (zoom >= 10) return images[10]
    return null
}

function getActiveTile(zoom) {
    if (zoom >= 5 && zoom < 8) return 5
    if (zoom >= 8 && zoom < 12) return 8
    if (zoom >= 12) return 12
    return 'None'
}

function updateMapInfo() {
    const mapInfo = document.getElementById('mapInfo')
    const zoom = map.getZoom()
    const activeTile = getActiveTile(zoom)
    mapInfo.innerHTML = `<h4>Map Info</h4><b>Zoom level:</b> ${zoom}<br><b>Active tile:</b> ${activeTile}`
}
