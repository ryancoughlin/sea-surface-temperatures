import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import morgan from 'morgan';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import apiRoutes from './src/routes/index.js';
import { REGIONS } from './src/config/regions.js';
import path from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();

app.use(express.json());
app.use(cors());
app.use(morgan('combined'));
app.use(express.static(join(__dirname, process.env.PUBLIC_DIR)));
app.use('/api', apiRoutes);

// Update the regions endpoint
app.get('/api/regions', (req, res) => {
  console.log('Regions requested');
  res.json(REGIONS);
});

app.get('/health', (req, res) => {
  console.log('Health check requested');
  res.status(200).json({ status: 'OK' });
});

// Add this new route before your catch-all route
app.get('/deck-prototype', (req, res) => {
  res.sendFile(path.join(__dirname, `${process.env.PUBLIC_DIR}/deck-prototype.html`));
});

app.use((err, req, res, next) => {
  console.error('Error caught in error handling middleware:', err.stack);
  res.status(500).json({ error: "Internal Server Error", details: err.message });
});

app.listen(process.env.PORT, () => {
  console.log(`Server is running on http://localhost:${process.env.PORT}`);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

export default app;
