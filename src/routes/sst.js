import express from 'express';
import { fetchSSTData } from '../services/sstService.js';

const router = express.Router();

router.get('/:source/:satellite/:region/:date', async (req, res) => {
  try {
    const { source, satellite, region, date } = req.params;
    const { metadata } = await fetchSSTData(source, satellite, region, date);
    res.json({ message: 'SST data processed successfully', metadata });
  } catch (error) {
    console.error('Error in SST route:', error);
    res.status(500).json({ error: error.message });
  }
});

export default router;
