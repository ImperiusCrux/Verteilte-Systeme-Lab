# Quelle: https://medium.com/fintechexplained/kafka-and-python-producer-consumer-833bdff3241e
import time
from kafka import KafkaProducer
from random import randrange
import json

timestamp = time.time()

from kafka.producer.kafka import log

# Producer erstellen
producer = KafkaProducer(bootstrap_servers=['10.50.15.52:9092'])
topicName = 'vlvs_inf20A_RoccNStone'


def generate_value():
    value = {"Nitra": randrange(500), "Gold": randrange(500), "Mushroom": randrange(500)}
    return json.dumps(value).encode('utf-8')


def on_success(record):
    print(record.partition)
    print(record.offset)


def on_error(excp):
    print("error:")
    log.error(excp)
    raise Exception(excp)


# alle 20 Sekunden neue Werte senden
while True:
    producer.send(topicName, generate_value()).add_callback(on_success).add_errback(on_error)
    # blockieren, bis asynchronen Nachrichten gesendet wurden
    producer.flush()
    time.sleep(20)
