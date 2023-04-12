import json
import threading
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish

name = "default"


# Setup with subscription to default, publication of client state and will for /aichat/clientstate
def on_connect(client, userdata, flags, rc):
    try:
        client.subscribe('/aichat/default')
        will = {'topic': "/aichat/clientstate", 'payload': "Client Python Peter has disconnected", 'qos': 2, 'retain': True}
        publish.single("/aichat/clientstate", "Chat client Python Peter started.", will=will, hostname='10.50.12.150')
        print('Connected')
    except Exception as e:
        print(e)
        print("Connection Error please restart.")
        return


# Processing of messages in subscribed channels as topic~sender: text
def on_message(client, userdata, msg):
    try:
        message_json = msg.payload.decode('utf8')  # extracts json from binary string
        content_dict = json.loads(message_json)  # converts to dictonary
        print(
            content_dict['topic'] + '~' + content_dict['sender'] + ': ' + content_dict['text'] + '\n'
        )
    except Exception as e:
        print(e)
        print("Incoming message couldn't be processed.")
        return


# helper function to set username used for publishing messages
def setUser():
    print("Bitte Nutzername eingeben:")
    global name
    name = input()


# input function for messages sent by user
def getInput():
    try:
        global name
        print("Type Message: " + "\n")
        message = input()
        # payload assembly
        test = '{"sender": "' + name + '", "text": "' + message + '", "clientId": "Python Peter", "topic": "default"}'
        client.publish('/aichat/default', test, qos=2, retain=False)
        getInput()
    except Exception as e:
        print(e)
        print("Message couldn't be send. Please restart")
        return


# setup for username and connection
setUser()
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect('10.50.12.150', 1883, 60)

# extra routine to keep the input running while displaying incoming messages
thread = threading.Thread(target=getInput)
thread.start()
client.loop_forever()
