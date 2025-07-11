# Use the official Eclipse Mosquitto image as base
FROM eclipse-mosquitto:latest

# Expose the MQTT port
EXPOSE 1883

# Copy your custom Mosquitto configuration file into the container
COPY mosquitto.conf /mosquitto/config/mosquitto.conf

# Command to start Mosquitto with your config
CMD ["/usr/sbin/mosquitto", "-c", "/mosquitto/config/mosquitto.conf"]
