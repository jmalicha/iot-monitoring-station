#!/bin/bash

# Ensure the InfluxDB persistent storage directory exists
# This directory will store InfluxDB data to prevent data loss on container restarts
mkdir -p influxdb/persistent/influxdb

# Start the services using Docker Compose in detached mode (-d)
# This will launch InfluxDB and Grafana as per the docker-compose.yml configuration
docker compose up -d