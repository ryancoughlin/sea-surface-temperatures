import express from 'express';
import { fetchSSTData } from '../services/sstService.js';

const router = express.Router();

router.get('/:source/:satellite/:region/:date', async (req, res) => {
  console.log('Route accessed with params:', req.params);
  try {
    const { source, satellite, region, date } = req.params;

    console.log(`Fetching SST data for source: ${source}, satellite: ${satellite}, region: ${region}, date: ${date}`);

    const { data, metadata } = await fetchSSTData(source, satellite, region, date);

    console.log('Data fetched successfully, metadata:', metadata);

    res.setHeader('Content-Type', 'application/x-netcdf');
    res.setHeader('Content-Disposition', `attachment; filename=sst_data_${region}_${metadata.date}.nc4`);
    res.send(data);
  } catch (error) {
    console.error('Error in SST route:', error);
    const statusCode = error.message.includes('NC4 file not found') ? 404 : 500;
    res.status(statusCode).json({ error: error.message });
  }
});

export default router;
