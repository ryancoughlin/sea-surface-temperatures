import { DATA_SOURCES } from '../config/sources.js';

export function formatDateToOrdinal(dateString) {
  const date = new Date(dateString);
  const start = new Date(date.getFullYear(), 0, 0);
  const diff = date - start;
  const oneDay = 1000 * 60 * 60 * 24;
  const dayOfYear = Math.floor(diff / oneDay);
  return `${date.getFullYear()}${dayOfYear.toString().padStart(3, '0')}`;
}

export function validateInputs(source, satellite, region, date) {
  const sourceConfig = DATA_SOURCES[source];
  
  if (!sourceConfig) {
    throw new Error(`Invalid source: ${source}`);
  }

  const satelliteConfig = sourceConfig.satellites[satellite];
  if (!satelliteConfig) {
    throw new Error(`Invalid satellite for source ${source}: ${satellite}`);
  }

  if (!sourceConfig.regions.includes(region)) {
    throw new Error(`Invalid region for source ${source}: ${region}`);
  }

  if (!date) {
    throw new Error('Date is required');
  }
}

export function constructUrl(source, satellite, region, date) {
    const formattedDate = formatDateToOrdinal(date);
  const sourceConfig = DATA_SOURCES[source];
  const satelliteConfig = sourceConfig.satellites[satellite];
  
  const filename = satelliteConfig.file_format(formattedDate, 'daily', region);
  return `${sourceConfig.base_url}/${satellite}/${satelliteConfig.measurement.toLowerCase()}/daily/${region}/${filename}`;
}
