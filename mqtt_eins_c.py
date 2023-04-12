import json
import paho.mqtt.client as mqtt


# connecting to client and subscribing to all topics in /weather/
def on_connect(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))
    client.subscribe('/weather/#')


# parsing incoming messages via dictionary for output
def on_message(client, userdata, msg):
    message_json = msg.payload.decode('utf8')  # extracts json from binary string
    content_dict = json.loads(message_json)  # converts to dictonary
    print(
        'StadtId: ' + str(content_dict['cityId']) + '\n',
        'Stadt: ' + content_dict['city'] + '\n',
        'Aktuelle Temperatur: ' + str(content_dict['tempCurrent']) + '\n',
        'Maximale Temperatur: ' + str(content_dict['tempMax']) + '\n',
        'Minimale Temperatur: ' + str(content_dict['tempMin']) + '\n',
        'Zeitstempel: ' + content_dict['timeStamp'] + '\n',
        'Kommentar: ' + content_dict['comment'],
    )


# setup for connection
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect('10.50.12.150', 1883, 60)
client.loop_forever()
