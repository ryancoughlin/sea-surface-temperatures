const config = {
  regionURLs: {
    GM: "https://eastcoast.coastwatch.noaa.gov/data/avhrr-viirs/sst-ngt/daily/gm/",
    MR: "https://eastcoast.coastwatch.noaa.gov/data/avhrr-viirs/sst-ngt/daily/mr/",
    NL: "https://eastcoast.coastwatch.noaa.gov/data/avhrr-viirs/sst-ngt/daily/nl/",
  },
  dataDirectory: "./data",
  cronSchedule: "0 0 * * *",
  timezone: "America/New_York",
  fileExtension: ".nc4",
  daysBack: 1,
};

export default config;
