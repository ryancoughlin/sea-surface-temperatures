import axios from 'axios';
import * as turf from '@turf/turf';
import config from '../config/index.js';
import { getTimeRange } from '../utils/dateUtils.js';

const celsiusToFahrenheit = (celsius) => (celsius * 9) / 5 + 32;

const createFeature = (row) => {
  const [time, lat, lon, temp] = row;
  if (temp === null) return null;  // Return null for features with null temperature

  return {
    type: 'Feature',
    geometry: {
      type: 'Point',
      coordinates: [parseFloat(lon), parseFloat(lat)]
    },
    properties: {
      temperature: celsiusToFahrenheit(parseFloat(temp)),
      time: time
    }
  };
};

const interpolateTemperature = (features, [lon, lat]) => {
  const nearestPoints = turf.nearestPoint(
    turf.point([lon, lat]),
    turf.featureCollection(features)
  );

  if (!nearestPoints || !nearestPoints.properties) {
    return null;
  }

  return nearestPoints.properties.temperature;
};

const createInterpolatedFeature = (features, [lon, lat], time) => ({
  type: 'Feature',
  geometry: {
    type: 'Point',
    coordinates: [lon, lat]
  },
  properties: {
    temperature: interpolateTemperature(features, [lon, lat]),
    time,
    interpolated: true
  }
});

const generateGrid = (bbox, step) => {
  const grid = [];
  for (let lon = bbox[0]; lon <= bbox[2]; lon += step) {
    for (let lat = bbox[1]; lat <= bbox[3]; lat += step) {
      grid.push([lon, lat]);
    }
  }
  return grid;
};

const dateToSeconds = (dateString) => {
  const date = new Date(dateString);
  return Math.floor(date.getTime() / 1000);
};

const formatDate = (date) => {
  return date.toISOString().split('.')[0] + 'Z';
};

export const processSST = (sstData) => {
  if (!sstData || !sstData.table || !Array.isArray(sstData.table.rows) || sstData.table.rows.length === 0) {
    console.error('Invalid or empty SST data received:', sstData);
    throw new Error('Invalid or empty SST data received');
  }

  const captureDate = new Date(sstData.table.rows[0][0]);
  const features = sstData.table.rows
    .map(createFeature)
    .filter(feature => feature !== null);  // Filter out null features

  if (features.length === 0) {
    throw new Error('No valid SST data points found');
  }

  const featureCollection = turf.featureCollection(features);
  const bbox = turf.bbox(featureCollection);
  const gridStep = 0.1;
  const grid = generateGrid(bbox, gridStep);

  const interpolatedFeatures = grid.map(point => 
    createInterpolatedFeature(features, point, captureDate.toISOString())
  ).filter(feature => feature.properties.temperature !== null);

  return {
    type: 'FeatureCollection',
    properties: {
      captureDate: captureDate.toISOString()
    },
    features: [...features, ...interpolatedFeatures]
  };
};

export const fetchSSTData = async (region, date) => {
  const { BASE_URL, DATASET_ID, RESPONSE_FORMAT, VARIABLE } = config;
  const { minLat, maxLat, minLon, maxLon } = region.coordinates;
  
  // Get current date and set it to the current time
  const now = new Date();

  // Set the maximum available date (based on the error message)
  const maxAvailableDate = new Date('2024-10-17T12:00:00Z');

  // If no date is provided, use yesterday's date
  if (!date) {
    const yesterday = new Date(now);
    yesterday.setDate(yesterday.getDate() - 1);
    date = formatDate(yesterday);
  } else {
    // Ensure the provided date is in the correct format
    date = new Date(date).toISOString().split('.')[0] + 'Z';
  }

  // Ensure the date is not in the future and not beyond the maximum available date
  let requestDate = new Date(date);
  if (requestDate > now || requestDate > maxAvailableDate) {
    requestDate = new Date(Math.min(now, maxAvailableDate));
    date = formatDate(requestDate);
  }

  console.log('Using date for SST data:', date);

  // Set the time range for the entire day of the requested date
  const startDate = new Date(requestDate);
  startDate.setUTCHours(0, 0, 0, 0);
  const endDate = new Date(requestDate);
  endDate.setUTCHours(23, 59, 59, 999);

  const startDateString = formatDate(startDate);
  const endDateString = formatDate(endDate);
  
  const url = `${BASE_URL}${DATASET_ID}${RESPONSE_FORMAT}?${VARIABLE}[(${startDateString}):1:(${endDateString})][(${minLat}):1:(${maxLat})][(${minLon}):1:(${maxLon})]`;
  console.log('Fetching SST data from URL:', url);
  
  try {
    const response = await axios.get(url);
    console.log('Received response from NOAA API');
    const processedData = processSST(response.data);
    return processedData;
  } catch (error) {
    console.error('Error fetching or processing SST data:', error.response ? error.response.data : error.message);
    throw error;
  }
};
