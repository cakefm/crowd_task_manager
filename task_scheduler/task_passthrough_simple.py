import sys
sys.path.append("..")
from common.settings import cfg
import common.tree_tools as tt
import common.file_system_manager as fsm
import time
import requests
import pika
import json
import numpy.random as rnd
from bson.objectid import ObjectId
import xml.dom.minidom as xml
import uuid

from pymongo import MongoClient


def callback(channel, method, properties, body):

    message = json.loads(body)
    task_id = message["task_id"]
    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})
    task_type_name = task["type"]
    task_type = db[cfg.col_task_type].find_one({"name": task_type_name})
    task_step_name = task["step"]
    threshold = task_type["steps"][task_step_name]["min_responses"]
    result_type = task_type["steps"][task_step_name]["result_type"]

    tree = xml.parseString(task['xml']).documentElement

    print(f"Processing task {task_id} of type {task_type} at step {task_step_name}; providing result type {result_type}")

    clefs = [
        tt.create_element_node("clef", {"shape":"G", "line":"4"}),
        tt.create_element_node("clef", {"shape":"F", "line":"3"}),
        tt.create_element_node("clef", {"shape":"C", "line":"2"})
    ]

    for i in range(threshold):
        treeClone = tree.cloneNode(deep=True)
        node = clefs[i]
        layer = treeClone.getElementsByTagName("layer")[0]
        layer.appendChild(node.cloneNode(deep=True))
        payload = treeClone.toxml()

        requests.post(f"http://localhost:443/{task['_id']}", data=payload)
        for j in range(0):  # Putting this to >0 will simulate delay in response
            connection.process_data_events()
            time.sleep(0.2)

    print(f"  Passed through task with ID {task['_id']} of type:step {task['type']}:{task['step']} as result {threshold} times")
    print(f"  - payload: {payload}")
    raise Exception("I've served my purpose...")


if __name__ == "__main__":
    processed = set()
    client = MongoClient(*cfg.mongodb_address)
    db = client[cfg.db_name]

    # Pika
    parameters = pika.ConnectionParameters(*cfg.rabbitmq_address)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=cfg.mq_task_passthrough)
    channel.basic_consume(
        on_message_callback=callback,
        queue=cfg.mq_task_passthrough,
        auto_ack=True
    )

    print('Task passthrough is listening...')
    channel.start_consuming()