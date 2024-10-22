import fs from 'fs/promises'
import path from 'path'

export async function saveFile(data, filename, dir = process.env.DATA_DIR) {
  const filePath = path.join(dir, filename)
  await fs.writeFile(filePath, data)
  return filePath
}

export async function moveFile(source, destination) {
  await fs.rename(source, destination)
}

export async function listFiles(dir) {
  return fs.readdir(dir)
}
