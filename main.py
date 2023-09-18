import time

from pymongo import MongoClient
from flask import Flask, request
import json
from kafka import KafkaProducer

app = Flask(__name__)
connection = MongoClient('mongodb://localhost:27020/')
producer = KafkaProducer(bootstrap_servers='localhost:9092')


@app.route('/scheduleTask', methods=['POST'])
def scheduleTask():
    seconds = request.form['seconds']
    id = time.time_ns()
    db = connection['task_db']
    collection = db['task_collection']
    collection.insert_one({"task_id":str(id),"status":"queued"})
    producer.send('my-topic', value=json.dumps({"id":id,"seconds":seconds}).encode('utf-8'))
    producer.flush()
    return {'success':id}

@app.route('/checkStatus', methods=['POST'])
def checkStatus():
    id = request.form['id']
    db = connection['task_db']
    collection = db['task_collection']
    document = collection.find_one({"task_id":str(id)})
    if document is None:
        return {'status': 'not scheduled'}

    return {'status': document['status']}



app.run(debug=True,port=5001)
