
import argparse
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
import logging
from time import sleep

# Initialize logger
logger = logging.getLogger(__name__)

def setup_logger():
    """
    Sets up the logger configuration for logging output.
    Logs timestamps and messages with a specific format.
    """
    logging.basicConfig(
        datefmt='%Y-%m-%d %H:%M:%S',  # Date and time format for log output
        format='%(asctime)s %(message)s',  # Log message format
        level=logging.INFO  # Log level set to INFO
    )

def main(args: argparse.Namespace):
    """
    Main function that continuously queries InfluxDB for data in the last 5 minutes 
    and logs the results.

    Args:
        args (argparse.Namespace): Command-line arguments parsed by `parse_arguments`.
    """
    setup_logger()  # Set up logging configuration

    # Initialize InfluxDB client with connection parameters
    client = InfluxDBClient(
        host=args.host,
        database=args.db,
        username=args.username,
        password=args.password,
    )

    # Infinite loop to repeatedly fetch data every 5 seconds
    while True:
        # Function to convert datetime to InfluxDB timestamp (in nanoseconds)
        def get_influx_timestamp(t: datetime) -> int:
            return int(t.timestamp() * 10 ** 9)  # Convert to nanoseconds
        
        # Get timestamps for the last 5 minutes and the current time
        start_timestamp = get_influx_timestamp(datetime.now() - timedelta(minutes=5))
        end_timestamp = get_influx_timestamp(datetime.now())

        # Query InfluxDB for data in the last 5 minutes
        query = f'SELECT * FROM {args.measurement} WHERE time > {start_timestamp} AND time < {end_timestamp}'
        results = client.query(query)

        # Convert query results into a list of entries
        results = list(results.get_points())
        logger.info(f"Read {len(results)} messages from InfluxDB in the last 5 minutes. Last 5 messages: ")

        # Get the last 5 results if there are more than 5 messages
        results = results if len(results) < 5 else results[-5:]

        # Log the result data
        for result in results:
            logger.info(result)

        # Wait for 5 seconds before the next query
        sleep(5)

def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser("InfluxDB Writer")  # Create argument parser
    parser.add_argument(
        "--host",
        required=False,
        type=str,
        default="localhost",  # Default host is localhost
        help="Host address for InfluxDB server"
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
    parser.add_argument(
        "--measurement",
        required=False,
        type=str,
        default="temperature",  # Default measurement is "temperature"
        help="Measurement to read from InfluxDB"
    )
    return parser.parse_args()  # Return the parsed arguments

if __name__ == "__main__":
    # Parse arguments and run the main function
    main(parse_arguments())
