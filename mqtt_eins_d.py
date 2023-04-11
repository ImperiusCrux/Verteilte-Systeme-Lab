import json
import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))
    client.subscribe('/aichat/default')


def on_message(client, userdata, msg):
    print(msg.topic)
    print(msg.payload)


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect('10.50.12.150', 1883, 60)
client.loop_forever()
