import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import morgan from 'morgan';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import apiRoutes from './src/routes/index.js';
import { errorHandler, notFoundHandler } from './src/middleware/errorHandlers.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

const app = express();
const port = process.env.PORT || 3000;

app.use(express.json());
app.use(cors());
app.use(morgan('dev'));

const publicPath = join(__dirname, process.env.PUBLIC_DIR || 'public');
app.use(express.static(publicPath));

app.use('/api', apiRoutes);

app.get('*', (req, res) => {
  res.sendFile(join(publicPath, 'index.html'));
});

app.use(notFoundHandler);
app.use(errorHandler);

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

export default app;
