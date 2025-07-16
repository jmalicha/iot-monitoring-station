# Use the official Python image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the receiver script
COPY receiver.py .

# Run the receiver script (connecting to mosquitto on 'mosquitto' host)
CMD ["python", "receiver.py", "--host", "mosquitto", "--topic", "test/topic"]
