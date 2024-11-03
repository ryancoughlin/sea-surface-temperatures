#!/bin/bash
git pull origin main

docker-compose down
docker network prune -f
docker-compose up --build -d

python3 main.py
