import app from './app.js';
import { PORT } from './src/config/index.js';

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
