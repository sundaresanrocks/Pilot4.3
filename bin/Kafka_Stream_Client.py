from flask import Flask, request, jsonify
import json
from flask_cors import CORS
from kafka import KafkaConsumer, KafkaProducer
import logging
import json
from flask import jsonify, request, make_response, abort
from app import app


class KafkaClient:
    def __init__(self):
        self.KAFKA_SERVER = app.config['KAFKA_SERVER']
        self.TOPIC_NAME = app.config['TOPIC_NAME']
        self.producer = KafkaProducer(
            bootstrap_servers=self.KAFKA_SERVER,
            api_version=(0, 11, 15)
        )

        @app.route('/kafka/pushToConsumers', methods=['POST'])
        def kafkaProducer():
            req = request.get_json()
            json_payload = json.dumps(req)
            json_payload = str.encode(json_payload)
            # push data into INFERENCE TOPIC
            self.producer.send(self.TOPIC_NAME, json_payload)
            self.producer.flush()
            print("Sent to consumer")
            return jsonify({
                "message": "Message Successfully Sent To Conumer",
                "status": "Pass"})


