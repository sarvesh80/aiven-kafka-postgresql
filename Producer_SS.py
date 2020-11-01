# Import KafkaProducer from Kafka library
from kafka import KafkaProducer
import os
import json

def get_data():

    data_file = open("email.json", "r")
    data = json.loads(data_file.read())
    return data

def get_kafka_ssl_info():

    ssl_info = {}
    ssl_info["ssl_cafile"] = os.environ["KAFKA_CA"]
    ssl_info["ssl_certfile"] = os.environ["KAFKA_SERVICE_CERT"]
    ssl_info["ssl_keyfile"] = os.environ["KAFKA_SERVICE_KEY"]

    return ssl_info

if __name__ == "__main__":
    # Define server with port
    bootstrap_servers = ['kafkaservicess-sarvesh-8ea2.aivencloud.com:26036']

    # Define topic name where the message will publish
    topicName = 'SS_Topic'

    ssl_info = get_kafka_ssl_info()

    # Initialize producer variable
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers,
        security_protocol="SSL",
        ssl_cafile=ssl_info["ssl_cafile"],
        ssl_certfile=ssl_info["ssl_certfile"],
        ssl_keyfile=ssl_info["ssl_keyfile"],
        value_serializer=lambda v: json.dumps(v).encode('ascii')
        )

    # read data from file and send each item there as a separate msg
    # Publish text in defined topic
    data = get_data()
    for item in data:
            producer.send(topicName, item)
    producer.flush()

    # Print message
    print("Message Sent to Topic")