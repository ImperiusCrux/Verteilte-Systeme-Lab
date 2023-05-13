from datetime import datetime
from datetime import timedelta
import dateutil.parser
import kafka
import json
import threading
import dateutil as du
import pandas as pd

topic = "tankerkoenig"


def create_consumer(partition_nr):
    consumer = kafka.KafkaConsumer(
        bootstrap_servers=['10.50.15.52:9092'],
        auto_offset_reset='earliest',
        api_version=(0, 10, 2),
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('ascii')))
    consumer.assign([kafka.TopicPartition(topic, partition_nr)])
    return consumer


fredList = []

def runKafkaBullshit(conNr=int):
    cons = create_consumer(conNr)
    check = None
    contentList = []
    for message in cons:
        if not containsBullshit(message):
            current = dateutil.parser.parse(message.value["dat"])
            if check is None:
                check = current
            if check + timedelta(hours=1) <= current:
                yeetContent(contentList, conNr)
                contentList = []
                check = dateutil.parser.parse(message.value["dat"])
            contentList.append(message.value)


# value={'pE5': 1.819, 'pE10': 1.759, 'dat': '2023-01-01T09:25:07.000+00:00',
# 'stat': '2c21b856-4850-0952-e100-00000630df04', 'plz': '12249', 'pDie': 1.889}


def containsBullshit(message):
    if message.value["dat"] is None or message.value["pE5"] is None or message.value["pE10"] is None or message.value["stat"] is None or message.value["plz"] is None or message.value["pDie"] is None:
        return True
    else:
        return False

def yeetContent(content, fred):
    e5 = []
    e10 = []
    die = []
    for msg in content:
        e5.append(msg.value["pE5"])
        e10.append(msg.value["pE10"])
        die.append(msg.value["pDie"])


for i in range(1, 10):
    fredList.append(threading.Thread(target=runKafkaBullshit, kwargs={"conNr": i}))

for freds in fredList:
    freds.start()



'''
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
'''


