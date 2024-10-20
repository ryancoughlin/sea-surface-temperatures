import express from 'express';
import { fetchSSTData } from '../services/sstService.js';
import { REGIONS } from '../config/regions.js';

const router = express.Router();

// SST data route
router.get('/sst/:region', async (req, res) => {
  try {
    const { region } = req.params;
    if (!REGIONS[region]) {
      return res.status(404).json({ error: 'Region not found' });
    }
    const sstData = await fetchSSTData(REGIONS[region]);
    res.json(sstData);
  } catch (error) {
    console.error('Error fetching SST data:', error);
    res.status(500).json({ error: 'Failed to fetch SST data' });
  }
});

// Regions route
router.get('/regions', (req, res) => {
  res.json(Object.values(REGIONS));
});

export default router;
