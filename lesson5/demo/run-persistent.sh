# Use Docker Compose to start the services defined in the "docker-compose-persistent.yaml" file.
# The "-f" option specifies which Compose file to use (in this case, "docker-compose-persistent.yaml").
# Note: The "docker-compose-persistent.yaml" file configures persistent volumes for data storage,
# which ensures that data generated inside the containers will persist even after container restarts or removals.
docker compose -f docker-compose-persistent.yaml up -d