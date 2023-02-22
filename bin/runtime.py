#########################
# Runtime Configuration #
#########################
from flask import Flask, jsonify, make_response
# TODO Future Implementation : Aggregation
runtime = Flask(__name__)

runtime.config['timestamp'] = 28800  # Unit Seconds (Time interval over which service set to run)

