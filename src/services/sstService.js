import { validateInputs, constructUrl } from '../utils/sstUtils.js';
import path from 'path';
import { saveFile, moveFile, listFiles } from './fileManager.js';
import { runPythonScript } from './pythonRunner.js';
import fs from 'fs/promises';

async function fetchData(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
  return response.arrayBuffer();
}

export async function fetchSSTData(source, satellite, region, date) {
  validateInputs(source, satellite, region, date);

  const url = constructUrl(source, satellite, region, date);
  console.log(`Fetching SST data from URL: ${url}`);

  try {
    const data = await fetchData(url);
    const filename = `${source}_${satellite}_${region}_${date}.nc4`;
    const nc4FilePath = await saveFile(Buffer.from(data), filename);

    console.log(`File saved to: ${nc4FilePath}`);
    
    // Check if file exists and log its size
    const stats = await fs.stat(nc4FilePath);
    console.log(`File size: ${stats.size} bytes`);

    const scriptPath = path.join(process.cwd(), 'generate-sst.py');
    const { stdout, stderr } = await runPythonScript(scriptPath, [nc4FilePath]);

    if (stderr) {
      console.error('Python script error:', stderr);
      throw new Error('SST processing failed: ' + stderr);
    }

    console.log('Python script output:', stdout);

    const files = await listFiles(process.env.OUTPUT_DIR);
    const movedFiles = [];
    for (const file of files) {
      if (file.startsWith('sst_zoom_') && file.endsWith('.png')) {
        const newPath = path.join(process.env.PUBLIC_DIR, file);
        await moveFile(
          path.join(process.env.OUTPUT_DIR, file),
          newPath
        );
        movedFiles.push(newPath);
      }
    }

    return {
      success: true,
      message: 'SST data processed successfully',
      metadata: { source, satellite, region, date, url },
      files: movedFiles
    };
  } catch (error) {
    console.error('Error in fetchSSTData:', error);
    throw error;
  }
}
