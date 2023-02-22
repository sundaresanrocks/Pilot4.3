import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
import random
import numpy as np
from app import app

# pip install kafka-python

KAFKA_TOPIC_NAME_CONS = app.config['TOPIC_NAME']
KAFKA_BOOTSTRAP_SERVERS_CONS = app.config['KAFKA_SERVER']

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")
    print("KAFKA_BOOTSTRAP_SERVERS_CONS : {}".format(KAFKA_BOOTSTRAP_SERVERS_CONS))
    print("KAFKA_TOPIC_NAME_CONS : {}".format(KAFKA_TOPIC_NAME_CONS))
    # kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
    #                                    api_version=(0, 11, 15),
    #                                    value_serializer=lambda x: x.encode('utf-8'))
    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       api_version=(0, 11, 15),
                                       value_serializer=lambda x: x.encode('utf-8'))

    filepath = "../data/20210509_classification.csv"

    classification_df = pd.read_csv(filepath, sep=';')

    classification_df['order_id'] = np.arange(len(classification_df))

    flower_list = classification_df.to_dict(orient="records")

    message_list = []
    message = None
    for message in flower_list:
        message_fields_value_list = [message["day"],
                                     message["animal"],
                                     message["lie"],
                                     message["stand"],
                                     message["walk"],
                                     message["ruminate"],
                                     message["grazing"],
                                     message["nothing"],
                                     message["inactive"],
                                     message["active"],
                                     message["highActive"]]

        message = ','.join(str(v) for v in message_fields_value_list)
        print("Message Type: ", type(message))
        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        # KafkaProducer(bootstrap_servers=['localhost:9092'],
        #               api_version=(0, 11, 5),
        #               value_serializer=lambda x: dumps(x).encode('utf-8'))
        time.sleep(1)

    print("Kafka Producer Application Completed. ")