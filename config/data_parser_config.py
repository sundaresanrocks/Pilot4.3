from flask import Flask, request, jsonify
app = Flask(__name__)
# os.environ.get('', '')

app.config['DATA_SOURCE'] = "localhost"
app.config['DATA_FILE_PATH'] = "../../data/"
app.config['DATA_SOURCE_TYPE'] = ".csv"
