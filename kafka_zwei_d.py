# Script um Wetter-Daten in eine mySQL Datenbank zu bekommen, um diese in Grafana abzubilden
# Quelle: https://levelup.gitconnected.com/visualize-your-data-from-mysql-in-a-matter-of-minutes-with-grafana-54cf3e63a160
# Quelle: https://www.w3schools.com/python/python_mysql_create_db.asp

# imports für MySQL-Datenbank
import mysql.connector
# Imports für kafka-Consumer
from kafka import KafkaConsumer, TopicPartition
import json


# ----------------------------------------------------------------------------------------------------------------------
# Erstellen der MySQL-Datenbank
# ----------------------------------------------------------------------------------------------------------------------
class MySQLDatabase:
    def __init__(self, db_name):
        self.db_name = db_name
        self.table_name = "weatherTable"
        self.cursor = None
        self.mydb = None
        # erstele db
        self.create_database()
        # erstelle Tabelle
        self.create_weather_table()

    # Funktion, um db zu erstellen
    def create_database(self):
        mydb = mysql.connector.connect(
            host="localhost",
            user="RoccNStone",
            passwd="Nitra",
        )
        cursor = mydb.cursor()

        # Falls die DB noch nicht existiert, erstelle diese
        try:
            cursor.execute("CREATE DATABASE " + self.db_name)
        except Exception as ex:
            print(ex)

    # Funktion, um Wetter Tabelle zu erstellen
    def create_weather_table(self):
        self.mydb = mysql.connector.connect(
            host="localhost",
            user="RoccNStone",
            passwd="Nitra",
            database=self.db_name
        )

        self.cursor = self.mydb.cursor()

        # Falls die Tabelle bereits existiert: lösche diese und erstelle eine neue
        self.cursor.execute(f"SHOW TABLES LIKE '{self.table_name}'")
        if self.cursor.fetchone():
            self.cursor.execute("DROP TABLE " + self.table_name)
        # Erstelle die Tabelle
        self.cursor.execute(
            "CREATE TABLE " + self.table_name + "(id INT AUTO_INCREMENT PRIMARY KEY, tempCurrent DECIMAL(8,6), "
                                                "tempMax "
                                                "DECIMAL(8,6),tempMin DECIMAL(8,6), comment VARCHAR(255), "
                                                "timeStamp DATETIME, city VARCHAR(255), cityId INT)")

    # Funktion, um Tabelle Einträge hinzuzufügen
    def create_table_entry(self, content_dict):
        sql = "INSERT INTO " + self.table_name + "(tempCurrent, tempMax, tempMin, comment, timeStamp, city, cityId) " \
                                                 "VALUES (%s, %s, " \
                                                 "%s, %s, %s, %s, %s) "
        val = (content_dict['tempCurrent'], content_dict['tempMax'], content_dict['tempMin'], content_dict['comment'],
               content_dict['timeStamp'], content_dict['city'], content_dict['cityId'])
        self.cursor.execute(sql, val)
        self.mydb.commit()
        print("Record inserted")


# MySql-DB erstellen
db = MySQLDatabase("RoccNStone_Database")

# ----------------------------------------------------------------------------------------------------------------------
# Erstellen des Consumers
# ----------------------------------------------------------------------------------------------------------------------

consumer = KafkaConsumer(
    'weather',
    bootstrap_servers=['10.50.15.52:9092'],
    api_version=(0, 10, 2),
    enable_auto_commit=False,
    auto_offset_reset='earliest',
    group_id='RoccNStone-group',
    value_deserializer=lambda m: json.loads(m.decode('ascii')))

for message in consumer:
    content_dict = message.value
    db.create_table_entry(content_dict)
    # commit, um offset abzuspeichern
    consumer.commit()

consumer.close()
