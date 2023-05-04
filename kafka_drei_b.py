from datetime import datetime

from kafka import KafkaConsumer, TopicPartition
import json
import threading
import pandas as pd

topic = "tankerkoenig"


def create_consumer(partition_nr):
    consumer = KafkaConsumer(
        bootstrap_servers=['10.50.15.52:9092'],
        auto_offset_reset='earliest',
        api_version=(0, 10, 2),
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('ascii')))
    consumer.assign([TopicPartition(topic, partition_nr)])
    return consumer


consumer_list = []

for i in range(0, 10):
    consumer_list.append(create_consumer(i))


def create_df(value):
    df = pd.Series(value)
    print(df)
    return df


def structure_data(consumer):
    # erst checken ob iwas NUll ist
    tank_dict = {'pE5': [],
                 'pE10': [],
                 'dat': [],
                 'stat': [],
                 'plz': [],
                 'pDie': []}

    # erst checken ob iwas NUll ist
    msg = next(consumer)

    for key, value in msg.value.items():
        tank_dict[key].append(value)

    first_timestamp = datetime.strptime(tank_dict["dat"][-1], "%Y-%m-%dT%H:%M:%S.%f%z")
    last_timestamp = datetime.strptime(tank_dict["dat"][0], "%Y-%m-%dT%H:%M:%S.%f%z")
    time_difference = first_timestamp-last_timestamp

    if time_difference.total_seconds() <= 3600:
        print("Die Dauer ist kleiner als eine Stunde.")
    #     msg = next(consumer)
   #     print("Die Dauer ist kleiner als eine Stunde.")
   #     dict.update(msg.value)
    #    print(dict)
   # else:
        # Aggregation pro Preiskategorie
     #   return
    return


structure_data(consumer_list[0])

