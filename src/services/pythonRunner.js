import { spawn } from 'child_process'

export function runPythonScript(scriptPath, args) {
  return new Promise((resolve, reject) => {
    const process = spawn(ENV.PYTHON_PATH, [scriptPath, ...args])
    let output = ''

    process.stdout.on('data', (data) => {
      output += data.toString()
      console.log(`Python output: ${data}`)
    })

    process.stderr.on('data', (data) => {
      console.error(`Python error: ${data}`)
    })

    process.on('close', (code) => {
      if (code !== 0) reject(new Error(`Python script exited with code ${code}`))
      else resolve(output)
    })
  })
}
