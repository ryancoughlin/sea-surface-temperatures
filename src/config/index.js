import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

dotenv.config({ path: new URL('../../.env', import.meta.url) });

export const PORT = process.env.PORT || 3000;
export const BASE_URL = "https://coastwatch.noaa.gov/erddap/griddap/";
export const DATASET_ID = "noaacwBLENDEDsstDNDaily";
export const RESPONSE_FORMAT = ".json";
export const VARIABLE = "analysed_sst";

export default {
  PORT,
  BASE_URL,
  DATASET_ID,
  RESPONSE_FORMAT,
  VARIABLE,
};
