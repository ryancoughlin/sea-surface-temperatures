#!/bin/bash

# Exit on any error
set -e

# Check if required directories exist
if [ ! -d "assets" ]; then
    echo "Error: assets directory not found"
    exit 1
fi

if [ ! -d "output" ]; then
    echo "Creating output directory..."
    mkdir -p output
fi

# Pull the latest code from the repository
git pull origin main

# Stop containers and clean up
echo "Stopping containers and cleaning up..."
docker-compose down
docker system prune -f  # Remove unused data

# Rebuild and start containers
echo "Building and starting containers..."
docker-compose up --build -d

# Verify containers are running
echo "Verifying containers..."
sleep 5
docker-compose ps