# aiven-kafka-postgresql

Attribution:

https://www.programcreek.com/python/example/98440/

https://help.aiven.io/en/articles/489572-getting-started-with-aiven-kafka


Kafka-PostgreSQL Produce/Consumer App
This is a Kafka sample app that runs a producer which publishes data to a topic that is consumed by consumer and inserted into the PostgreSQL database.

### Pre-requisities

Python (v3.7+)

Kafka (v2.6)

PostgreSQL (v12.4)

pgAdmin (v4.27) (https://www.pgadmin.org/)

### Environment Variables (Ctrl+ R-> sysdm.cpl)

Set System variables for KAFKA_CA, KAFKA_SERVICE_CERT, KAFKA_SERVICE_KEY and specify the file locations of each of the corresponding variables

KAFKA_CA - ca.pem

KAFKA_SERVICE_CERT - service.cert

KAFKA_SERVICE_KEY - service.key

POSTGRES_URI - URI for PostgreSQL database

### Installation
Run the code below first to get necessary libraries installed for Python

pip install --user --requirement requirements.txt

Create table schema in  PostgreSQL database using pgAdmin

(Data is supplied in email.json file)

### How to run
python Producer_SS.py

python Consumer_SS.py

Run producer app first.

### How to run coverage report
coverage run -a Producer_SS.py

coverage run -a Consumer_SS.py

coverage report
