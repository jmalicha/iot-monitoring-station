#!/bin/bash

# Stop all running container and delete all images generate by docker compose
docker compose -f docker-compose-ephemeral.yaml down --rmi all
docker compose -f docker-compose-persistent.yaml down --rmi all