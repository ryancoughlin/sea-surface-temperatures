import { formatDateToOrdinal, validateInputs, constructUrl } from '../utils/sstUtils.js';
import path from 'path';
import { saveFile, moveFile, listFiles } from './fileManager.js';
import { runPythonScript } from './pythonRunner.js';


async function fetchData(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
  return response.arrayBuffer();
}

export async function fetchSSTData(source, satellite, region, date) {
  validateInputs(source, satellite, region, date);

  const url = constructUrl(source, satellite, region, date);
  console.log(`Fetching SST data from URL: ${url}`);

  const data = await fetchData(url);
  const nc4FilePath = await saveFile(Buffer.from(data), 'capecod.nc4');

  const scriptPath = path.join(process.cwd(), 'generate-sst.py');
  await runPythonScript(scriptPath, []);

  const files = await listFiles(ENV.OUTPUT_DIR);
  for (const file of files) {
    if (file.startsWith('sst_zoom_') && file.endsWith('.png')) {
      await moveFile(
        path.join(ENV.OUTPUT_DIR, file),
        path.join(ENV.PUBLIC_DIR, file)
      );
    }
  }

  return {
    metadata: { source, satellite, region, date, url }
  };
}
