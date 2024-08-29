import logging
import requests
import json
from time import sleep
from datetime import datetime
from kafka import KafkaProducer
import config

kafka_server = [config.server]
topic = "test_topic"

producer = KafkaProducer(
    bootstrap_servers = kafka_server,
    value_serializer = lambda v: json.dumps(v).encode("utf-8"),
)

def main():
    logging.info("START")
    for i in range(170000):
        response = requests.get("http://localhost:8000/data").json()
        print(response)
        producer.send(topic, response)
        producer.flush()
        sleep(3)
    logging.debug("GOT %s",  response)

main()