import { promisify } from 'util'
import { exec } from 'child_process'
import path from 'path'
import fs from 'fs/promises'

const execPromise = promisify(exec)

export async function runPythonScript(scriptPath, args) {
  try {
    const pythonPath = process.env.PYTHON_PATH || 'python3'
    const fullScriptPath = path.resolve(scriptPath)
    const command = `${pythonPath} "${fullScriptPath}" ${args.join(' ')}`
    
    console.log(`Executing command: ${command}`)
    
    const { stdout, stderr } = await execPromise(command, {
      env: { ...process.env, PYTHONUNBUFFERED: '1' },
      cwd: path.dirname(fullScriptPath),
      timeout: 300000 // 5 minutes timeout
    })
    
    if (stderr) {
      console.error(`Python stderr: ${stderr}`)
    }
    
    console.log(`Python output: ${stdout}`)
    return { stdout: stdout.trim(), stderr: stderr.trim() }
  } catch (error) {
    console.error(`Python error: ${error.message}`)
    console.error(`Error details: ${JSON.stringify(error, null, 2)}`)
    
    // Check if the file exists
    try {
      await fs.access(args[0])
      console.log(`File ${args[0]} exists`)
    } catch (e) {
      console.error(`File ${args[0]} does not exist or is not accessible`)
    }
    
    throw new Error(`Python script execution failed: ${error.message}`)
  }
}
