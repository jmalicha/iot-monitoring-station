#!/bin/bash

# Use Docker Compose to start the services defined in the "docker-compose-ephemeral.yaml" file.
# The "-f" option specifies which Compose file to use (in this case, "docker-compose-ephemeral.yaml").
# Note: The "docker-compose-ephemeral.yaml" file does not configure persistent storage (no volumes are defined),
# which means that any data generated inside the containers will be lost when the containers are stopped or removed.
# This setup is typically used for temporary or short-lived tasks where data persistence is not required.
docker compose -f docker-compose-ephemeral.yaml up -d