import express from 'express';
import { usEastCoastRegions } from '../config/index.js';
import { fetchSSTData, processSST } from '../services/sstService.js';

const router = express.Router();

router.get('/:region/:date?', async (req, res, next) => {
  try {
    const requestedRegion = req.params.region;
    const requestedDate = req.params.date; // This can be undefined or in YYYY-MM-DDTHH:mm:ssZ format

    console.log('Requested region:', requestedRegion);
    console.log('Requested date:', requestedDate || 'Not provided, will use last available date');

    const region = usEastCoastRegions.find(r => r.slug === requestedRegion);

    if (!region) {
      console.log('Invalid region requested:', requestedRegion);
      return res.status(400).json({ error: "Invalid region" });
    }

    console.log('Fetching SST data for region:', region, 'and date:', requestedDate || 'to be determined');
    const sstData = await fetchSSTData(region, requestedDate);

    console.log('SST data fetched for date:', sstData.properties.captureDate);
    
    res.json(sstData);
  } catch (error) {
    console.error('Error in /sst/:region/:date? route:', error);
    res.status(500).json({ error: error.message || 'An unexpected error occurred' });
  }
});

export default router;
