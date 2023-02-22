from flask import Flask, request, jsonify
import json
from kafka import KafkaConsumer, KafkaProducer

app = Flask(__name__)
# os.environ.get('', '')

app.config['KAFKA_SERVER'] = "172.21.0.3:9092"
app.config['TOPIC_NAME'] = "INFERENCE"
