import argparse
from datetime import datetime
from influxdb import InfluxDBClient
import json
import logging
import paho.mqtt.client as mqtt
import typing

# Configure logger
logger = logging.getLogger(__name__)

# Timestamp format
TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"


def setup_logger():
    # Set up logging format
    logging.basicConfig(
        datefmt='%Y-%m-%d %H:%M:%S',
        format='%(asctime)s %(message)s',
        level=logging.INFO
    )


def create_mqtt_client(
    host: str,
    port: int = 1883,
    keep_alive: int = 60,
    on_connect: typing.Callable = None,
    on_message: typing.Callable = None,
    on_disconnect: typing.Callable = None,
    userdata: typing.Any = None,
) -> 'Client':
    """
    Creates and configures an MQTT client using the Paho MQTT library.

    This function initializes a new MQTT client, sets optional callback 
    functions for connection, message reception, and disconnection events, 
    and connects to the specified broker.

    Parameters:
    -----------
    host : str
        The hostname or IP address of the MQTT broker to connect to.
    port : int, optional
        The port number of the MQTT broker. Default is 1883.
    keep_alive : int, optional
        Maximum interval (in seconds) between communications to keep the connection alive.
    on_connect : Callable, optional
        Callback function to handle the connection event.
    on_message : Callable, optional
        Callback function to handle incoming messages.
    on_disconnect : Callable, optional
        Callback function to handle disconnection events.
    userdata: Any, optional
        Custom object passed by the caller that will be received in the on_message callback

    Returns:
    --------
    Client
        An instance of the Paho MQTT `Client` that is connected to the broker.

    Notes:
    ------
    - This function will attempt to connect to the broker immediately.
    - Call `client.loop_forever()` or `client.loop_start()` to maintain the connection.
    """
    client = mqtt.Client(userdata=userdata)

    # Set callbacks if provided
    if on_connect:
        client.on_connect = on_connect
    if on_message:
        client.on_message = on_message
    if on_disconnect:
        client.on_disconnect = on_disconnect

    # Connect to the broker
    client.connect(host, port=port, keepalive=keep_alive)

    return client


def create_mqtt_receiver(
    host: str,
    port: int = 1883,
    keep_alive: int = 60,
    userdata: typing.Any = None,
) -> 'Client':
    """
    Creates an MQTT client specifically for receiving messages from a broker.

    This function sets up default callback functions for:
    - Logging a message when the client connects to the broker.
    - Logging any message received from the broker.
    - Logging when the client disconnects.

    Parameters:
    -----------
    host : str
        The MQTT broker's hostname or IP address.
    port : int, optional
        The port number of the MQTT broker. Default is 1883.
    keep_alive : int, optional
        Keepalive time in seconds. Default is 60.
    userdata: Any, optional
        Custom object passed by the caller that will be received in the on_message callback

    Returns:
    --------
    Client
        A configured and connected MQTT client.
    """
    logger.info(f"Creating MQTT receiver: host={host}, port={port}")

    # Define callback when client connects
    def on_connect(client, userdata, flags, reason_code):
        logger.info("[on_connect] Connected to the broker")

    # Define callback when a message is received
    def on_message(client, userdata, message):
        influxdb_client = userdata

        topic = message.topic
        if len(topic.split("/")) != 2:
            logger.warning(f"Received message with invalid topic={topic}")
            return

        # Deduce location and station from message topic
        location, station = topic.split("/")

        payload = json.loads(message.payload.decode())
        logger.info(f"[on_message] Received message: '{payload}' on topic: '{topic}'")

        messages = []
        for measurement in payload:
            if 'timestamp' not in payload[measurement]:
                logger.warning(f"Received message without timestamp for location={location}, " +
                               f"station={station}, measurement={measurement}")
                continue

            timestamp = payload[measurement]['timestamp']

            value = None or payload[measurement].get('value')
            if not isinstance(value, float):
                logger.warning(f"Received non-float value={value} for location={location}, " +
                               f"station={station}, measurement={measurement}")
                continue

            messages.append({
                "timestamp": datetime.strptime(timestamp, TIMESTAMP_FMT),
                "measurement": measurement,
                "tags": {
                    "location": location,
                    "station": station,
                },
                "fields": {
                    "value": value,
                }
            })

        if messages:
            influxdb_client.write_points(messages)

    # Define callback when the client disconnects
    def on_disconnect(client, userdata, reason_code):
        logger.info("[on_disconnect] Disconnected from the broker")

    # Create and return the MQTT client
    return create_mqtt_client(
        host,
        port=port,
        keep_alive=keep_alive,
        on_connect=on_connect,
        on_message=on_message,
        on_disconnect=on_disconnect,
        userdata=userdata,
    )


def main(args: argparse.Namespace):
    """
    Main function that initializes the MQTT receiver and subscribes to a topic.

    This function is triggered when the script runs and:
    - Configures logging.
    - Creates an MQTT receiver.
    - Subscribes to the specified topic.
    - Starts the MQTT event loop.

    Parameters:
    -----------
    args : argparse.Namespace
        Parsed command-line arguments.
    """
    setup_logger()

    # Create the InfluxDB client
    influx_client = InfluxDBClient(
        host=args.influxdb_host,
        database=args.influxdb_database,
        username=args.influxdb_username,
        password=args.influxdb_password,
    )

    # Create an MQTT receiver with provided arguments
    receiver = create_mqtt_receiver(
        args.mqtt_broker_host,
        port=args.mqtt_broker_port,
        userdata=influx_client,
    )

    # We don't know the topics in advance, we should subscribe to all of them
    receiver.subscribe("#")

    # Start the client event loop
    logger.info("Listening for incoming messages. Press CTRL+C to exit.")
    # This call will keep the broker connection alive and is a blocking call (i.e. the program will stop at this point)
    receiver.loop_forever()


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments for the MQTT receiver.

    Returns:
    --------
    argparse.Namespace
        A namespace containing the parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="MQTT Receiver Demo"
    )
    parser.add_argument(
        "--mqtt-broker-host",
        required=False,
        type=str,
        default="localhost",
        help="The MQTT broker hostname or IP address. Default: localhost"
    )
    parser.add_argument(
        "--mqtt-broker-port",
        required=False,
        type=int,
        default=1883,
        help="The network port of the MQTT broker. Default: 1883"
    )
    parser.add_argument(
        "--influxdb-host",
        required=False,
        type=str,
        default="localhost",  # Default host is localhost
        help="InfluxDB host address"
    )
    parser.add_argument(
        "--influxdb-database",
        required=False,
        type=str,
        default="iot_monitoring_station",  # Default database
        help="Name of the InfluxDB database"
    )
    parser.add_argument(
        "--influxdb-username",
        required=False,
        type=str,
        default="root",  # Default username is "root"
        help="Username for InfluxDB authentication"
    )
    parser.add_argument(
        "--influxdb-password",
        required=False,
        type=str,
        default="root",  # Default password is "root"
        help="Password for InfluxDB authentication"
    )
    return parser.parse_args()


if __name__ == "__main__":
    # Start the script by parsing arguments and running the main function
    main(parse_arguments())