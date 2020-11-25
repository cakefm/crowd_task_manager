import sys
sys.path.append("..")
from common.settings import cfg
import common.tree_tools as tt
import common.file_system_manager as fsm
import time
import requests
import pika
import json
from bson.objectid import ObjectId
import xml.dom.minidom as xml

from pymongo import MongoClient


def callback(channel, method, properties, body):
    message = json.loads(body)     
    task = db[cfg.col_task].find_one({"_id": ObjectId(message["task_id"])})
    task_type = db[cfg.col_task_type].find_one({"name": task["type"]})
    task_step = task["step"]
    threshold = task_type["steps"][task_step]["min_responses"]

    payload = task['xml']
    tree = xml.parseString(payload).documentElement

    if task["type"]=="0_check_skeleton":
        node = tree.childNodes[0].cloneNode(deep=True)
        modified_tree = tree.appendChild(node).parentNode
        payload = modified_tree.toprettyxml()
    elif task["type"]=="1_detect_clefs":
        node = tt.create_element_node("clef", {"shape":"G", "line":"2"})
        modified_tree = tt.insert_node(tree, [0], node, append=True) # in first measure
        payload = modified_tree.toprettyxml()
    
    for i in range(threshold):
        requests.post(f"http://localhost:443/{task['_id']}", data=payload)
        for j in range(2):
            connection.process_data_events()
            time.sleep(0.2)
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