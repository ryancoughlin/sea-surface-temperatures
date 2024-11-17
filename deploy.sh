#!/bin/bash

# Pull the latest code from the repository
git pull origin main

# Stop and restart Docker containers with the latest code
docker-compose down
docker-compose up --build -d