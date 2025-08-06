import argparse
from datetime import datetime
from influxdb import InfluxDBClient
import logging
import random
from time import sleep
from typing import Dict, Optional

# Initialize logger for the script
logger = logging.getLogger(__name__)

# Define the measurement parameters (temperature, humidity, battery) and their bounds
MEASUREMENTS = {
    "temperature": {
        "lower_bound": 20,
        "upper_bound": 30,
    },
    "humidity": {
        "lower_bound": 30,
        "upper_bound": 70,
    },
    "battery": {
        "lower_bound": 50,
        "upper_bound": 100,
    }
}

# List of possible locations and stations
STATIONS = {
    "London (UK)": {
        "stations": ["iot-uk-01", "iot-uk-02"],
    },
    "New York (US)": {
        "stations": ["iot-us-01", "iot-us-02", "iot-us-03"],
    },
    "Tokyo (JP)": {
        "stations": ["iot-jp-01"],
    },
}


def setup_logger():
    """
    Sets up the logging configuration for the script.
    Logs messages with timestamp and specified format at the INFO level.
    """
    logging.basicConfig(
        datefmt='%Y-%m-%d %H:%M:%S',  # Date format in log messages
        format='%(asctime)s %(message)s',  # Format for log messages
        level=logging.INFO  # Set log level to INFO
    )


def build_message(
    timestamp: Optional[datetime] = None,
    measurement: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    value: float = None,
) -> dict:
    """
    Builds a message (data point) to be written to InfluxDB.

    Args:
        timestamp (Optional[datetime]): The timestamp for the message. Defaults to current time if None.
        measurement (Optional[str]): The type of measurement (e.g., "temperature"). Randomly selected if None.
        tags (Optional[Dict[str, str]]): Tags for the data point (e.g., location, station). Randomly chosen if None.
        value (float): The value of the measurement. Randomly generated within bounds if None.

    Returns:
        dict: The formatted message as a dictionary.
    """
    timestamp = timestamp or datetime.now()  # Use current time if no timestamp is provided

    # Randomly choose a measurement if not provided
    measurement = measurement or random.choice(list(MEASUREMENTS.keys()))
    assert measurement in MEASUREMENTS, f"Measurement {measurement} is unknown"

    # Set default tags if not provided
    if not tags:
        # Choose a random location
        location = random.choice(list(STATIONS.keys()))
        tags = {
            "location": location,
            # Choose a random station for that location
            "station": random.choice(STATIONS[location]["stations"]),
        }

    # Generate a random value within the defined bounds for the measurement
    lower_bound = MEASUREMENTS[measurement]["lower_bound"]
    upper_bound = MEASUREMENTS[measurement]["upper_bound"]
    value = value or random.uniform(lower_bound, upper_bound)
    value = round(value, 2)  # Round the value to two decimal places

    # Create the message to be written to InfluxDB
    message = {
        "timestamp": timestamp.strftime('%Y-%m-%dT%H:%M:%S'),  # Convert timestamp to string format
        "measurement": measurement,  # Measurement type (e.g., "temperature")
        "tags": dict(**tags),  # Tags for the data point
        "fields": {
            "value": value,  # The value of the measurement
        }
    }

    return message


def main(args: argparse.Namespace):
    """
    Main function to connect to InfluxDB, generate and send random data to the database every 5 seconds.

    Args:
        args (argparse.Namespace): The command-line arguments parsed by `parse_arguments()`.
    """
    setup_logger()  # Set up logging

    # Connect to InfluxDB using credentials provided via arguments
    logger.info(f"Connecting to InfluxDB: host={args.host}, db={args.db}")
    client = InfluxDBClient(
        host=args.host,
        database=args.db,
        username=args.username,
        password=args.password,
    )

    # Infinite loop to generate and send data periodically
    while True:
        messages = []
        # Generate messages for each measurement type
        for measurement in MEASUREMENTS.keys():
            messages.append(build_message(measurement=measurement))

        # Log how many messages will be written
        logger.info(f"Writing {len(messages)} messages to InfluxDB.")
        
        # Write the generated messages to InfluxDB
        client.write_points(messages)
        
        # Wait for 5 seconds before generating and sending the next set of messages
        sleep(5)


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments for InfluxDB connection details.

    Returns:
        argparse.Namespace: The parsed arguments (host, db, username, password).
    """
    parser = argparse.ArgumentParser("InfluxDB Writer")  # Create argument parser
    parser.add_argument(
        "--host",
        required=False,
        type=str,
        default="localhost",  # Default host is localhost
        help="InfluxDB host address"
    )
    parser.add_argument(
        "--db",
        required=False,
        type=str,
        default="iot_monitoring_station",  # Default database
        help="Name of the InfluxDB database"
    )
    parser.add_argument(
        "--username",
        required=False,
        type=str,
        default="root",  # Default username is "root"
        help="Username for InfluxDB authentication"
    )
    parser.add_argument(
        "--password",
        required=False,
        type=str,
        default="root",  # Default password is "root"
        help="Password for InfluxDB authentication"
    )
    return parser.parse_args()  # Return the parsed arguments


if __name__ == "__main__":
    # Parse arguments and run the main function
    main(parse_arguments())