#!/bin/bash

# Exit on any error
set -e

# Required directories
REQUIRED_DIRS=(
    "static"
    "static/vector_tiles"
    "static/region_thumbnails"
    "api"
    "data"
)

# Create required directories
echo "Setting up directory structure..."
for dir in "${REQUIRED_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        echo "Creating $dir directory..."
        mkdir -p "$dir"
        chmod 777 "$dir"  # Ensure write permissions
    fi
done

# Pull the latest code from the repository
echo "Pulling latest code..."
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

# Verify nginx configuration
echo "Verifying nginx configuration..."
docker-compose exec nginx nginx -t

echo "Deployment complete! 🚀"