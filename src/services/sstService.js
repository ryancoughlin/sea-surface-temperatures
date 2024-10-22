import axios from 'axios';
import * as turf from '@turf/turf';
import { spawn } from 'child_process';
import NodeCache from 'node-cache';
import { formatDateToOrdinal, validateInputs, constructUrl } from '../utils/sstUtils.js';

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

function runPythonScript(scriptPath, args) {
  return new Promise((resolve, reject) => {
    const process = spawn('python', [scriptPath, ...args]);
    const chunks = [];

    process.stdout.on('data', (data) => {
      chunks.push(data);
    });

    process.stderr.on('data', (data) => {
      console.error(`Error: ${data}`);
    });

    process.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`Python script exited with code ${code}`));
      } else {
        resolve(Buffer.concat(chunks));
      }
    });
  });
}

export async function processSST(sstData) {
  // Implement any necessary processing of the SST data
  // This might be used if you need to do any data manipulation before sending to the client
}

const cache = new NodeCache({ stdTTL: 3600 * 24 }); // Cache for 1 hour

async function fetchData(url) {
  try {
    const response = await axios.get(url, { responseType: 'arraybuffer' });
    console.log(`Successfully fetched SST data from URL: ${url}`);
    return Buffer.from(response.data);
  } catch (error) {
    handleFetchError(error, url);
  }
}

function handleFetchError(error, url) {
  if (error.response) {
    if (error.response.status === 404) {
      console.error(`NC4 file not found at URL: ${url}`);
      throw new Error(`NC4 file not found. The data might not be available for this date.`);
    } else {
      console.error(`Failed to fetch NC4 data: ${error.response.status} ${error.response.statusText}`);
      throw new Error(`Failed to fetch NC4 data: ${error.response.status} ${error.response.statusText}`);
    }
  } else if (error.request) {
    console.error(`No response received from the server for URL: ${url}`);
    throw new Error(`No response received from the server. Please check your internet connection and try again.`);
  } else {
    console.error(`Error setting up the request: ${error.message}`);
    throw new Error(`Error setting up the request: ${error.message}`);
  }
}

export async function fetchSSTData(source, satellite, region, date) {
  validateInputs(source, satellite, region, date);

  const formattedDate = formatDateToOrdinal(date);
  const url = constructUrl(source, satellite, region, formattedDate);

  console.log(`Attempting to fetch SST data from URL: ${url}`);

  const data = await fetchData(url);

  return {
    data,
    metadata: { source, satellite, region, date: formattedDate, url }
  };
}
