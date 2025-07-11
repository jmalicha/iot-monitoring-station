import argparse
import logging
import paho.mqtt.client as mqtt
from time import sleep
import typing

# Configure logger
logger = logging.getLogger(__name__)


def create_mqtt_client(
    host: str,
    port: int = 1883,
    keep_alive: int = 60,
    on_connect: typing.Callable = None,
    on_disconnect: typing.Callable = None,
) -> 'Client':
    """
    Creates and configures an MQTT client for sending messages.

    This function initializes a new MQTT client, sets optional callback 
    functions for connection and disconnection events, and connects to the 
    specified broker.

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
    on_disconnect : Callable, optional
        Callback function to handle disconnection events.

    Returns:
    --------
    Client
        An instance of the Paho MQTT `Client` that is connected to the broker.

    Notes:
    ------
    - This function will attempt to connect to the broker immediately.
    - Call `client.loop_start()` to handle the communication loop.
    """
    client = mqtt.Client()

    # Set callbacks if provided
    if on_connect:
        client.on_connect = on_connect
    if on_disconnect:
        client.on_disconnect = on_disconnect

    # Connect to the broker
    client.connect("localhost", 1883, 60)

    return client


def create_mqtt_sender(
    host: str,
    port: int = 1883,
    keep_alive: int = 60,
) -> 'Client':
    """
    Creates an MQTT client specifically for sending messages to a broker.

    This function sets up default callback functions for:
    - Logging a message when the client connects to the broker.
    - Logging when the client disconnects.

    Parameters:
    -----------
    host : str
        The MQTT broker's hostname or IP address.
    port : int, optional
        The port number of the MQTT broker. Default is 1883.
    keep_alive : int, optional
        Keepalive time in seconds. Default is 60.

    Returns:
    --------
    Client
        A configured and connected MQTT client.
    """
    logger.info(f"Creating MQTT sender: host={host}, port={port}")

    # Define callback when client connects
    def on_connect(client, userdata, flags, reason_code):
        logger.info("[on_connect] Connected to the broker")

    # Define callback when client disconnects
    def on_disconnect(client, userdata, reason_code):
        logger.info("[on_disconnect] Disconnected from the broker")

    # Create and return the MQTT client
    return create_mqtt_client(
        host,
        port=port,
        keep_alive=keep_alive,
        on_connect=on_connect,
        on_disconnect=on_disconnect,
    )


def main(args: argparse.Namespace):
    """
    Main function that initializes the MQTT sender and publishes messages.

    This function is triggered when the script runs and:
    - Configures logging.
    - Creates an MQTT sender.
    - Publishes the specified message to the topic.
    - Starts the MQTT event loop.
    
    Parameters:
    -----------
    args : argparse.Namespace
        Parsed command-line arguments.
    """
    # Set up logging format
    logging.basicConfig(
        datefmt='%Y-%m-%d %H:%M:%S',
        format='%(asctime)s %(message)s',
        level=logging.INFO
    )

    # Create an MQTT sender with provided arguments
    sender = create_mqtt_sender(
        args.host,
        port=args.port,
    )

    # Start the client event loop
    # This will create a background thread which keeps the broker connection alive
    # As opposed to loop_forever this call is non-blocking, which means that the program won't stop at this point
    sender.loop_start()
    
    while True:
        # Publish the message to the specified MQTT topic
        logger.info(f"Publishing message: '{args.message}' to topic: {args.topic}")
        sender.publish(args.topic, args.message)

        # Sleep for 10s before re-sending a message
        sleep(10)


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments for the MQTT sender.

    Returns:
    --------
    argparse.Namespace
        A namespace containing the parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="MQTT Sender Demo - Sends messages to a broker"
    )

    parser.add_argument(
        "--host",
        required=False,
        type=str,
        default="localhost",
        help="The MQTT broker's hostname or IP address. Default: localhost"
    )

    parser.add_argument(
        "--port",
        required=False,
        type=int,
        default=1883,
        help="The network port of the MQTT broker. Default: 1883"
    )

    parser.add_argument(
        "--topic",
        required=True,
        type=str,
        help="The MQTT topic to publish the message to."
    )

    parser.add_argument(
        "--message",
        required=False,
        type=str,
        default="IoT Monitoring Station",
        help="The message to send to the topic."
    )

    return parser.parse_args()


if __name__ == "__main__":
    # Start the script by parsing arguments and running the main function
    main(parse_arguments())