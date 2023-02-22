# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
from app import app
# Import sys module
import sys

# Define server with port
KAFKA_SERVER = app.config['KAFKA_SERVER']

bootstrap_servers = [KAFKA_SERVER]

# Define topic name from where the message will recieve
topicName = app.config['TOPIC_NAME']

# Initialize consumer variable
consumer = KafkaConsumer(topicName, bootstrap_servers=bootstrap_servers)

# Read and print message from consumer
for msg in consumer:
    print("Topic Name=%s,Message=%s"%(msg.topic,msg.value))


# Terminate the script
sys.exit()