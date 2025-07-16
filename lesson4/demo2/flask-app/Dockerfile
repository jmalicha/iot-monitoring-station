# Use the official Python image from Docker Hub as a base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt into the container
COPY requirements.txt /app/

# Install the dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy all the files from the local app directory in the container app directory
COPY app/ /app/

# Expose port 5000 for the Flask application
EXPOSE 5000

# Command to run the Flask app
CMD ["python", "server.py"]