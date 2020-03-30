import sys
import pika
import json
import numpy as np

sys.path.append("..")
import common.settings as settings
import common.file_system_manager as fsm
import common.tree_alignment_distance as tad
import xml.dom.minidom as xml

from pymongo import MongoClient

def callback(ch, method, properties, body):
    data = json.loads(body)
    task_id = data['task_id']

    client = MongoClient(settings.mongo_address[0], int(settings.mongo_address[1]))
    db = client.trompa_test

    results = db[settings.result_collection_name].find_one({"task_id" : task_id})["results"]

    # Create all xml pairs
    pairs = {}
    for i in range(len(results)):
        for j in range(i + 1, len(results)):
            a = results[i]
            b = results[j]
            pairs[(i, j)] = (xml.parseString(a), xml.parseString(b))

    # Perform alignmnents
    distances = np.full((len(results), len(results)), np.inf)
    for i, j in pairs:
        a, b = pairs[i, j]
        distances[i, j] = tad.align_trees(a, b)
    
    distance_bins = [0] * len(results)
    for i, j in pairs:
        distance_bins[i] += distances[i, j]
        distance_bins[j] += distances[i, j]

    # Tree with the smallest overall distance will be a good candidate
    main_tree_index = np.argmin(distance_bins)



    # global channel
    # channel.queue_declare(queue="status_queue")
    # channel.basic_publish(exchange="", routing_key="status_queue", body=json.dumps(status_update_msg))

address = settings.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address[0], address[1]))
channel = connection.channel()
channel.queue_declare(queue=settings.aggregator_queue_name)
channel.basic_consume(queue=settings.aggregator_queue_name, on_message_callback=callback, auto_ack=True)

print('Aggregator is listening...')
channel.start_consuming()