const { exec } = require("child_process");

function uploadToMapbox(file) {
  // Ensure that file names are safe to use in a shell command
  if (!/^[a-zA-Z0-9\-_.]+$/.test(file)) {
    console.error("Invalid filename, upload aborted.");
    return Promise.reject(new Error("Invalid filename"));
  }

  return new Promise((resolve, reject) => {
    const command = `mapbox upload yourusername.${file} ${file}`;
    exec(command, (error, stdout, stderr) => {
      if (error) {
        console.error(`Upload error: ${stderr}`);
        reject(new Error("Failed to upload file to Mapbox."));
      } else {
        console.log(`Upload stdout: ${stdout}`);
        resolve();
      }
    });
  });
}

module.exports = { uploadToMapbox };
