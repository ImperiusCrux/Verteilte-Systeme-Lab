from datetime import datetime
from datetime import timedelta
from statistics import mean
import dateutil.parser
import kafka
import json
import threading



# Imports für InfluxDB
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import influxdb_client, os, time

#-----------------------------------------------------------------------------------------------------------
# Konfigurationen der Zeitseriendatenbank InfluxDB, in welche die Daten geschreiben werden
#-----------------------------------------------------------------------------------------------------------

# InfluxDB (Time Series DB) Informationen, um in die DB zu schreiben
INFLUXDB_TOKEN = "a2j0fmPdCjj_XSlInYq_aShClRn87gSEGBJVZJIfSpBcdOspujHiMfDHFVipVglU4FcS7z7HaavTPBzfKHZ6nA=="
INFLUXDB_ORG = "DHBW_RoccNStone"
INFLUXDB_URL = "http://localhost:8086"
# Name der Datenbank/Bucket, in welches die Daten geschrieben werden
INFLUXDB_BUCKET = "Tankerkoenig"
# Initialisiern des Clients, um in influxDB zu schreiben
db_write_client = influxdb_client.InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
# Definieren der API zum schreiben der Daten
write_api = db_write_client.write_api(write_options=SYNCHRONOUS)


topic = "tankerkoenig"

# creating consumer with variable partition nr
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

# 1 function call per thread. Gets itself a consumer and parses incoming messages by partition and time into list
def runPartyConsumer(conNr=int):
    cons = create_consumer(conNr)
    check = None
    contentList = []
    for message in cons:
        # check for null values
        if not containsNone(message):
            # getting time value of first message as check
            current = dateutil.parser.parse(message.value["dat"])
            if check is None:
                check = current
            # checking if time in incoming messages has passed 1 hour to create bundle of 1 hour
            if check + timedelta(hours=1) <= current:
                # giving content to db and resetting timer
                yeetContent(contentList, conNr)
                contentList = []
                check = dateutil.parser.parse(message.value["dat"])
            contentList.append(message.value)


# helper function to check for empty fields
def containsNone(message):
    if message.value["dat"] is None or message.value["pE5"] is None or message.value["pE10"] is None or message.value["stat"] is None or message.value["plz"] is None or message.value["pDie"] is None:
        return True
    else:
        return False


# extracting fuel prices of message bundle, calculating average and passing values to db
def yeetContent(content, fred):
    e5 = []
    e10 = []
    die = []
    for msg in content:
        e5.append(msg["pE5"])
        e10.append(msg["pE10"])
        die.append(msg["pDie"])

    # Berechenen der Mittelwerte
    e5av = mean(e5)
    e10av = mean(e10)
    dieav = mean(die)

    plz = msg["plz"][0]

    # Datum und Zeit als String
    date_string = msg["dat"]
    print(date_string)
    # Das Datum und die Zeit parsen
    date_time = datetime.fromisoformat(date_string)
    # Unix-Zeit (in Nanosekunden)
    timestamp = int(date_time.timestamp() * 1e9)

    # Datenbank-Eintrag erstellen
    data_point = influxdb_client.Point("Spritpreis"). \
        tag("plz", plz). \
        field("Avg-E5", e5av). \
        field("Avg-E10",  e10av). \
        field("Avg-Diesel", dieav).\
        time(timestamp)


    # Eintrag in DB schreiben
    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=data_point)
    time.sleep(1)


# creates threads for each partition and runs threads
for i in range(10):
    fredList.append(threading.Thread(target=runPartyConsumer(), kwargs={"conNr": i}))

for freds in fredList:
    freds.start()



