from kafka import KafkaConsumer
import json
from pymongo import MongoClient
from time import sleep

consumer = KafkaConsumer('my-topic',bootstrap_servers='localhost:9092')
connection = MongoClient('localhost:27020')
for message in consumer:
    task = json.loads(message.value.decode('utf-8'))
    id = task['id']
    seconds = int(task['seconds'])
    db = connection['task_db']
    collection = db['task_collection']
    print("Marking task with id : ", id, " as running")
    collection.update_one({'task_id':str(id)},{"$set":{"status":"running"}})
    try:
        sleep(seconds)
        print("Marking task with id : ",id," as completed")
        collection.update_one({'task_id':str(id)},{"$set":{'status':"completed"}})
    except Exception as e:
        print("Marking task with id : ", id, " as failed")
        collection.update_one({'task_id': str(id)}, {"$set": {'status': "failed"}})
        print("Task has failed, exception is thrown",e)



