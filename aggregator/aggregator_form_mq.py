import sys
import pika
import json
import numpy as np

sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
import xml.dom.minidom as xml

from pymongo import MongoClient

def callback(ch, method, properties, body):
    data = json.loads(body)
    task_id = data['task_id']

    client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
    db = client.trompa_test

    results = db[cfg.col_result].find_one({"task_id" : task_id})["results"]

    # For now, pretend all the individual results are strings and perform consensus
    results_dict = {}
    for result in results:
        if result in results_dict:
            results_dict[result] += 1
        else:
            results_dict[result] = 0

    most_ocurring = max(results_dict, key=results_dict.get)
    count = results_dict[most_ocurring]

    # Update task status
    status_update_msg = {
    '_id': task_id,
    'module': 'aggregator_form',
    'status': 'complete'
    }

    if count / len(results) < cfg.aggregator_form_threshold:
        status_update_msg['status'] = 'failed'
    
    global channel
    channel.queue_declare(queue=cfg.mq_task_status)
    channel.basic_publish(exchange="", routing_key=cfg.mq_task_status, body=json.dumps(status_update_msg))

address = cfg.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address.ip, address.port))
channel = connection.channel()
channel.queue_declare(queue=cfg.mq_aggregator_form)
channel.basic_consume(queue=cfg.mq_aggregator_form, on_message_callback=callback, auto_ack=True)

print('Form aggregator is listening...')
channel.start_consuming()