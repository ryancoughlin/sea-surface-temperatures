import express from 'express';
import { fetchSSTData } from '../services/sstService.js';
import { REGIONS } from '../config/regions.js';
import { setupPythonRoute } from './pythonProcess.js';
import sstRouter from './sst.js';  // Import the SST router

const router = express.Router();

router.use('/sst', sstRouter);

router.get('/regions', (req, res) => {
  res.json(Object.values(REGIONS));
});

// Set up the Python processing route
setupPythonRoute(router);

router.get('/test', (req, res) => {
  console.log('API test route accessed');
  res.json({ message: 'API is working' });
});

export default router;
