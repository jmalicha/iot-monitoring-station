import os
import paho.mqtt.client as mqtt

HOST = os.getenv("HOST", "localhost")
PORT = int(os.getenv("PORT", 1883))  # Optional
TOPIC = os.getenv("TOPIC", "default/topic")

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    print(f"Received message on {msg.topic}: {msg.payload.decode()}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(HOST, PORT, 60)
client.loop_forever()
