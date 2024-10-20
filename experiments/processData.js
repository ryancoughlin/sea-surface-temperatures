// processData.js
const { retrieveAndProcessFiles } = require("./retrieveNOAAData");
const { convertFile } = require("./convertFile");
const { uploadToMapbox } = require("../uploadMapData");
const config = require("./config");

async function processNOAAData() {
  try {
    console.log("Starting the NOAA data processing workflow...");

    // Step 1: Fetch data
    const downloadedFiles = await retrieveAndProcessFiles();
    console.log("Data fetching complete. Files downloaded:", downloadedFiles);

    // Step 2: Convert data to GeoJSON
    const geojsonFiles = [];
    for (const file of downloadedFiles) {
      const geojsonFile = await convertFile(file.region, file.filename);
      geojsonFiles.push(geojsonFile);
      console.log(`Conversion complete for ${file.filename}.`);
    }

    console.log(
      "Data conversion complete. GeoJSON files created:",
      geojsonFiles
    );

    // Step 3: Upload GeoJSON files to Mapbox
    const uploadResults = await uploadToMapbox(geojsonFiles);
    console.log("Files successfully uploaded to Mapbox:", uploadResults);

    console.log("All processes completed successfully.");
  } catch (error) {
    console.error(
      "An error occurred during the NOAA data processing workflow:",
      error
    );
  }
}

const cron = require("node-cron");
const schedule = config.cronSchedule;

cron.schedule(schedule, processNOAAData, {
  scheduled: true,
  timezone: config.timezone,
});

if (require.main === module) {
  console.log("Manual data processing initiated...");
  processNOAAData();
}

module.exports = processNOAAData;
