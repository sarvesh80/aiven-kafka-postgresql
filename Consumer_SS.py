# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
import os
import json
# Import sys module
import sys
import psycopg2

def get_kafka_details():
    """ get Kafka SSL details"""
    ssl_info = {}
    ssl_info["ssl_cafile"] = os.environ["KAFKA_CA"]
    ssl_info["ssl_certfile"] = os.environ["KAFKA_SERVICE_CERT"]
    ssl_info["ssl_keyfile"] = os.environ["KAFKA_SERVICE_KEY"]

    return ssl_info

def get_pg_connection():
    """ Establish connection to PostgreSQL database"""
    try:
        connection = psycopg2.connect(os.environ.get('POSTGRES_URI'))
        return connection

    except (Exception, psycopg2.Error) as error:
        raise RuntimeError(f"Error while connecting to PostgreSQL : {error}")

if __name__ == "__main__":

    # Define server with port
    bootstrap_servers = ['kafkaservicess-sarvesh-8ea2.aivencloud.com:26036']
    
    # insert values in Postgresql database
    # establish Posgres connection and setup database if not already
    pg_conn = get_pg_connection()
    
	# Define topic name from where the message will recieve
    topicName = 'SS_Topic'
    
    # get kafka details
    ssl_info = get_kafka_details()
    
    # initialize KafkaConsumer 
    consumer = KafkaConsumer(topicName,
        bootstrap_servers = bootstrap_servers,
        group_id ="SS_Group",
        security_protocol="SSL",
        ssl_cafile=ssl_info["ssl_cafile"],
        ssl_certfile=ssl_info["ssl_certfile"],
        ssl_keyfile=ssl_info["ssl_keyfile"],
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )    
    
    data = []

    # poll for the messages and store them in a list
    for _ in range(2):
        raw_msgs = consumer.poll(timeout_ms=5000)
        for tp, msgs in raw_msgs.items():
            for msg in msgs:
                data.append(msg.value)
    # if any messages received, then insert into database
    if(len(data) > 0):
        try:
            cursor = pg_conn.cursor()
            for item in data:
                # insert query based on the data received
                cursor.execute("INSERT INTO test (name, email) VALUES(%s, %s)", (item['name'],item['email']))
            cursor.close()
            pg_conn.commit()
        except (Exception, psycopg2.Error) as error:
            raise RuntimeError(
                "Error while inserting into PG database: {error}")
        finally:
            pg_conn.close()

    consumer.commit()