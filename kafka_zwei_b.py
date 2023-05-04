# Quelle: https://medium.com/fintechexplained/kafka-and-python-producer-consumer-833bdff3241e

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'weather',
    bootstrap_servers=['10.50.15.52:9092'],
    auto_offset_reset='latest',
    api_version=(0, 10, 2),
    enable_auto_commit=False,
    group_id='my-test-group',
    value_deserializer=lambda m: json.loads(m.decode('ascii')))


for message in consumer:
    content_dict = message.value
    print(
        'StadtId: ' + str(content_dict['cityId']) + '\n',
        'Stadt: ' + content_dict['city'] + '\n',
        'Aktuelle Temperatur: ' + str(content_dict['tempCurrent']) + '\n',
        'Maximale Temperatur: ' + str(content_dict['tempMax']) + '\n',
        'Minimale Temperatur: ' + str(content_dict['tempMin']) + '\n',
        'Zeitstempel: ' + content_dict['timeStamp'] + '\n',
        'Kommentar: ' + content_dict['comment'],
    )
