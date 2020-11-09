import sys
sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
import time
import requests
import pika
import json
from bson.objectid import ObjectId

from pymongo import MongoClient


def callback(channel, method, properties, body):
    message = json.loads(body)
    task = db[cfg.col_task].find_one({"_id": ObjectId(message["task_id"])})
    task_type = db[cfg.col_task_type].find_one({"name": task["type"]})
    task_step = task["step"]
    threshold = task_type["steps"][task_step]["min_responses"]

    payload = task['xml']
    for i in range(threshold):
        requests.post(f"http://localhost:443/{task['_id']}", data=payload)
        time.sleep(0.1) # Just to make sure nothing weird happens
    print(f"Passed through task with ID {task['_id']} as result {threshold} times")

if __name__ == "__main__":
    processed = set()
    client = MongoClient(*cfg.mongodb_address)
    db = client[cfg.db_name]

    # Pika
    parameters = pika.ConnectionParameters(*cfg.rabbitmq_address)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=cfg.mq_ce_communicator)
    channel.basic_consume(
        on_message_callback=callback, 
        queue=cfg.mq_ce_communicator, 
        auto_ack=True
    )

    print('Task passthrough is listening...')
    channel.start_consuming()