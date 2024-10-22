import { spawn } from 'child_process';

function runPythonScript(scriptPath, args) {
  return new Promise((resolve, reject) => {
    const process = spawn('python', [scriptPath, ...args]);
    const chunks = [];

    process.stdout.on('data', (data) => {
      chunks.push(data);
    });

    process.stderr.on('data', (data) => {
      console.error(`Error: ${data}`);
    });

    process.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`Python script exited with code ${code}`));
      } else {
        resolve(Buffer.concat(chunks));
      }
    });
  });
}

export function setupPythonRoute(app) {
  app.get('/process-sst', async (req, res) => {
    const { source, product, region, time_range, date } = req.query;

    if (!source || !product || !region || !time_range || !date) {
      return res.status(400).json({ error: 'Missing required parameters' });
    }

    try {
      const imageBuffer = await runPythonScript('src/process_sst.py', [
        source,
        product,
        region,
        time_range,
        date
      ]);
      
      res.contentType('image/png');
      res.send(imageBuffer);
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });
}
