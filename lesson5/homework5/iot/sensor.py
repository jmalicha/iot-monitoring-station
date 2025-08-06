import argparse
from datetime import datetime
import json
import logging
import paho.mqtt.client as mqtt
import random
from time import sleep
import typing

# Configure logger to display timestamp and messages
logger = logging.getLogger(__name__)

# Timestamp format used in messages
TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"

# Predefined stations, locations, and measurement ranges
STATIONS = {
    "London (UK)": {
        "stations": ["iot-uk-01", "iot-uk-02"],
        "measurements": {
            "temperature": {
                "lower_bound": 15,
                "upper_bound": 18,
            },
            "humidity": {
                "lower_bound": 30,
                "upper_bound": 40,
            },
            "battery": {
                "lower_bound": 50,
                "upper_bound": 60,
            }
        }
    },
    "New York (US)": {
        "stations": ["iot-us-01", "iot-us-02", "iot-us-03"],
        "measurements": {
            "temperature": {
                "lower_bound": 20,
                "upper_bound": 23,
            },
            "humidity": {
                "lower_bound": 45,
                "upper_bound": 50,
            },
            "battery": {
                "lower_bound": 65,
                "upper_bound": 75,
            }
        }
    },
    "Tokyo (JP)": {
        "stations": ["iot-jp-01"],
        "measurements": {
            "temperature": {
                "lower_bound": 25,
                "upper_bound": 30,
            },
            "humidity": {
                "lower_bound": 60,
                "upper_bound": 70,
            },
            "battery": {
                "lower_bound": 80,
                "upper_bound": 100,
            }
        }
    },
}


def setup_logger():
    """
    Configures the logger to display timestamp and messages.
    """
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
    on_disconnect: typing.Callable = None,
) -> 'Client':
    """
    Creates and returns an MQTT client configured with optional callbacks.

    Parameters:
    - host: MQTT broker address.
    - port: MQTT broker port (default: 1883).
    - keep_alive: Keep-alive interval in seconds (default: 60).
    - on_connect: Optional callback when connected.
    - on_disconnect: Optional callback when disconnected.

    Returns:
    - Connected MQTT client.
    """
    client = mqtt.Client()

    if on_connect:
        client.on_connect = on_connect
    if on_disconnect:
        client.on_disconnect = on_disconnect

    client.connect(host, port=port, keepalive=keep_alive)
    return client


def create_mqtt_sender(
    host: str,
    port: int = 1883,
    keep_alive: int = 60,
) -> 'Client':
    """
    Creates an MQTT client for sending messages to the broker.

    Sets default connection and disconnection handlers.

    Returns:
    - Connected MQTT client ready to publish messages.
    """
    logger.info(f"Creating MQTT sender: host={host}, port={port}")

    def on_connect(client, userdata, flags, reason_code):
        logger.info("[on_connect] Connected to the broker")

    def on_disconnect(client, userdata, reason_code):
        logger.info("[on_disconnect] Disconnected from the broker")

    return create_mqtt_client(
        host,
        port=port,
        keep_alive=keep_alive,
        on_connect=on_connect,
        on_disconnect=on_disconnect,
    )


def build_message() -> typing.Tuple[str, str]:
    """
    Builds a random IoT message with random values based on station data.

    Returns:
    - Tuple (topic, message) where topic is the MQTT topic string and
      message is the JSON-encoded message.
    """
    location = random.choice(list(STATIONS.keys()))
    station = random.choice(STATIONS[location]["stations"])
    topic = location + "/" + station

    measurements_config = STATIONS[location]["measurements"]

    message = {}
    for measurement in measurements_config:
        lower_bound = measurements_config[measurement]["lower_bound"]
        upper_bound = measurements_config[measurement]["upper_bound"]
        value = random.uniform(lower_bound, upper_bound)

        message[measurement] = {
            "timestamp": datetime.now().strftime(TIMESTAMP_FMT),
            "value": value,
        }

    return topic, json.dumps(message)


def main(args: argparse.Namespace):
    """
    Main function to publish messages to the MQTT broker.

    This function runs in an infinite loop, generating messages
    and publishing them to the broker.
    """
    setup_logger()

    sender = create_mqtt_sender(
        args.host,
        port=args.port,
    )

    sender.loop_start()

    while True:
        topic, message = build_message()
        logger.info(f"Publishing message: '{message}' to topic: {topic}")
        sender.publish(topic, message)
        sleep(5)


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    Returns:
    - Parsed command-line arguments as Namespace object.
    """
    parser = argparse.ArgumentParser(
        description="IoT Sensor"
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
    return parser.parse_args()


if __name__ == "__main__":
    # Start the MQTT message publisher
    main(parse_arguments())