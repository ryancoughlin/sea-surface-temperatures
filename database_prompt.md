Here's a detailed prompt summarizing our project and progress:
Project: Sea Surface Temperature (SST) Data Management System
Objective: Develop a system to fetch, process, store, and serve sea surface temperature data for various oceanic regions.
Current Implementation:

1. Database Schema:
   Regions table: Stores region information including name, code, and geographical bounds.
   SST_data table: Stores processed SST data including date, image path, source, satellite, and temperature range.
   Backend Services:
   SST Service: Handles saving and retrieving SST data.
   Region Service: Manages region information.
   SST Processor: Processes raw SST data and generates images.
   Queue System: Manages background processing of SST data.
   API Endpoints:
   POST /sst/process: Queues SST data processing.
   GET /sst/status/:jobId: Checks status of processing job.
   GET /sst: Retrieves SST data for specific parameters.
   GET /sst/latest: Fetches latest SST data for a region.
   GET /sst/range: Retrieves SST data for a date range.
   GET /sources: Lists available data sources and satellites.
   GET /regions: Provides list of available regions.
   Data Flow:
   Raw NC4 files are downloaded from external sources.
   Files are processed to generate SST images and extract temperature data.
   Processed data and image paths are stored in the database.
   API serves this data to the frontend application.
   Image Handling:
   SST images are stored in a file system.
   Image paths are stored in the database for quick retrieval.
   Next Steps:
1. Implement daily automated data fetching and processing.
   Develop frontend to display SST data on interactive maps.
   Optimize database queries for performance as data grows.
   Implement user authentication and authorization.
   Add support for additional data sources and regions as needed.
   This system is designed to efficiently manage SST data, allowing for scalability and easy addition of new data sources or regions. The separation of data processing and serving allows for responsive API endpoints while handling potentially time-consuming data processing in the background.
