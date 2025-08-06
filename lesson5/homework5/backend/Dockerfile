# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt into the container at /app
COPY requirements.txt /app/

# Install the required Python dependencies from the requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files into the container
COPY backend.py /app/

# Command to run the backend with CLI arguments passed from environment variables
CMD python backend.py \
    --mqtt-broker-host "${MQTT_BROKER_HOST}" \
    --mqtt-broker-port "${MQTT_BROKER_PORT}" \
    --influxdb-host "${INFLUXDB_HOST}" \
    --influxdb-database "${INFLUXDB_DATABASE}" \
    --influxdb-username "${INFLUXDB_USER}" \
    --influxdb-password "${INFLUXDB_PASSWORD}"

