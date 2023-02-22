from Kafka_Stream_Client import KafkaClient
from app import app

# Micro Services Config:
kafka_client = KafkaClient()

# Temporary Place Holders for other services.
#
# spark_stream , ML

if __name__ == '__main__':
    # start rest server
    app.run(host='0.0.0.0', port=5000, threaded=False, debug=False)
