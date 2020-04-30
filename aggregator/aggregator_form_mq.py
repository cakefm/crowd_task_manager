import sys
import pika
import json
import numpy as np

sys.path.append("..")
import common.settings as settings
import common.file_system_manager as fsm
import xml.dom.minidom as xml

from pymongo import MongoClient

def callback(ch, method, properties, body):
    data = json.loads(body)
    task_id = data['task_id']

    client = MongoClient(settings.mongo_address[0], int(settings.mongo_address[1]))
    db = client.trompa_test

    results = db[settings.result_collection_name].find_one({"task_id" : task_id})["results"]

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

    if count / len(results) < settings.aggregator_form_threshold:
        status_update_msg['status'] = 'failed'
    
    global channel
    channel.queue_declare(queue=settings.task_status_queue_name)
    channel.basic_publish(exchange="", routing_key=settings.task_status_queue_name, body=json.dumps(status_update_msg))

address = settings.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address[0], address[1]))
channel = connection.channel()
channel.queue_declare(queue=settings.aggregator_form_queue_name)
channel.basic_consume(queue=settings.aggregator_form_queue_name, on_message_callback=callback, auto_ack=True)

print('Form aggregator is listening...')
channel.start_consuming()