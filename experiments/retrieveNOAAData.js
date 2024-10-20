import axios from "axios";
import cheerio from "cheerio";
import path from "path";
import fs from "fs";
import { fileURLToPath } from 'url';
import config from "./config.js";
import isWithinDateRange from "../dateHelpers.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Downloads data from a URL to a specified output path
const fetchData = async (url, outputPath) => {
  const response = await axios({ url, responseType: "stream" });
  const writer = fs.createWriteStream(outputPath);
  response.data.pipe(writer);
  return new Promise((resolve, reject) => {
    writer.on("finish", resolve);
    writer.on("error", reject);
  });
};

// Ensures the directory for the output file exists
const ensureDirectoryExists = (directory) => {
  if (!fs.existsSync(directory)) {
    fs.mkdirSync(directory, { recursive: true });
  }
};

// Logs all URLs that are about to be downloaded
const logFileURLs = (links) => {
  console.log("Attempting to download the following files:");
  links.forEach((link) => console.log(link));
};

// Processes each region, downloads eligible files based on file extension and date
const processRegion = async (regionKey) => {
  const baseURL = config.regionURLs[regionKey];
  const { data } = await axios.get(baseURL);
  const $ = cheerio.load(data);
  const links = $("ul:first-of-type a")
    .map((_, el) =>
      $(el)
        .attr("href")
        .replace(/^https?:\/?\/?/, "")
    ) // Enhanced regex handling
    .get()
    .filter((href) => href && href.endsWith(config.fileExtension)) // Ensure link ends with .nc4
    .map((filename) => {
      const cleanedURL = `${baseURL}${filename}`;
      console.log(`URL before download: ${cleanedURL}`); // Log to check URL correctness
      return cleanedURL;
    });

  const files = [];
  for (const fileURL of links) {
    const outputPath = path.join(
      config.dataDirectory,
      regionKey,
      path.basename(fileURL)
    );
    ensureDirectoryExists(path.dirname(outputPath));
    console.log(`Downloading file from: ${fileURL}`); // Final log before download
    await fetchData(fileURL, outputPath);
    files.push(outputPath);
  }
  return files;
};

// Orchestrates the file retrieval process across all configured regions
const retrieveAndProcessFiles = async () => {
  const downloadedFiles = [];
  for (const regionKey in config.regionURLs) {
    const files = await processRegion(regionKey);
    downloadedFiles.push(...files);
  }
  return downloadedFiles;
};

export { retrieveAndProcessFiles };
