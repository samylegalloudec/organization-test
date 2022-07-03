# This file is meant to retrieve data from an API via Kafka.

# Reminder : Zookeeper is the cluster manager. The kafka server is called the broker. There are producer (those who produce the data, who go get it on the API) and the consumer, who reads the data.

from const import API_KEY
from kafka import KafkaProducer
import urllib.request
import time
import json

key = API_KEY
url = "https://api.jcdecaux.com/vls/v1/stations?&apiKey={}".format(key)

producer = KafkaProducer(bootstrap_servers="localhost:9092")

while True:
    response = urllib.request.urlopen(url)

    stations = json.loads(response.read().decode())

    for station in stations:
        producer.send("velib-stations", json.dumps(station).encode())
    
    print("{} Produced {} station records".format(time.time(), len(stations)))
    time.sleep(1)

