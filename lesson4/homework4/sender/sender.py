import os
import paho.mqtt.client as mqtt
import time

HOST = os.getenv("HOST", "mosquitto")
PORT = int(os.getenv("PORT", 1883))  # Optional fallback
TOPIC = os.getenv("TOPIC", "default/topic")
MESSAGE = os.getenv("MESSAGE", "Hello from sender")

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.publish(TOPIC, MESSAGE)
    print(f"Sent message: {MESSAGE}")
    time.sleep(1)
    client.disconnect()

client = mqtt.Client()
client.on_connect = on_connect

client.connect(HOST, PORT, 60)
client.loop_forever()
