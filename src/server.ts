import express from 'express'
import { spawn } from 'child_process'

const app = express()
const port = process.env.PORT || 3000

app.use(express.json())

function runPythonScript(scriptPath: string, args: string[]): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const process = spawn('python', [scriptPath, ...args])
    const chunks: Buffer[] = []

    process.stdout.on('data', (data) => {
      chunks.push(data)
    })

    process.stderr.on('data', (data) => {
      console.error(`Error: ${data}`)
    })

    process.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`Python script exited with code ${code}`))
      } else {
        resolve(Buffer.concat(chunks))
      }
    })
  })
}

app.get('/sst', async (req, res) => {
  const { source, product, region, time_range, date } = req.query

  if (!source || !product || !region || !time_range || !date) {
    return res.status(400).json({ error: 'Missing required parameters' })
  }

  try {
    const imageBuffer = await runPythonScript('src/process_sst.py', [
      source as string,
      product as string,
      region as string,
      time_range as string,
      date as string
    ])
    
    res.contentType('image/png')
    res.send(imageBuffer)
  } catch (error) {
    res.status(500).json({ error: error.message })
  }
})

app.listen(port, () => {
  console.log(`Server running on port ${port}`)
})
