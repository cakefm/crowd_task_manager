import sys
import pika
import json
import numpy as np

sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
import xml.dom.minidom as xml

from pymongo import MongoClient
from collections import Counter
from bson.objectid import ObjectId

def callback(ch, method, properties, body):
    client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
    db = client.trompa_test

    data = json.loads(body)
    task_id = data['task_id']
    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})

    results = []
    results_query = db[cfg.col_result].find({
        "task_id": task_id,
        "step": task["step"]})
    for result in results_query:
        results.append(result['result'])

    # Every entry in the results array should be a json with names for the field as keys
    # The values will be lists, even when just having single values (see field2 and field3)
    # Form schema will be derived from the first result
    # ex:
    # {
    #     "field1": ["a", "b", "c"],
    #     "field2": ["a"]
    # }
    #
    #
    
    # Count results
    response_count = len(results)
    form_fields = json.loads(results[0])
    counters = {field:Counter() for field in form_fields}
    for result in results:
        parsed = json.loads(result)
        for field in parsed:
            counters[field].update(parsed[field])

    # For each field determine the final results
    agg_result = dict()
    for field in counters:
        counter = counters[field]
        valid_results = []
        for choice in counter:
            consensus = (counter[choice] / response_count) > cfg.aggregator_form_threshold
            if consensus:
                valid_results.append(choice)
        agg_result[field] = valid_results

    # Update task status
    status_update_msg = {
    '_id': task_id,
    'module': 'aggregator_form',
    'status': 'complete'
    }

    # Check if none of the lists are empty
    # As we cannot yet send partial results, we need everyone to agree on all fields
    for field in agg_result:
        if not agg_result[field]:
            status_update_msg['status'] = 'failed'

    # If this is the case, put in db
    if not status_update_msg['status'] == 'failed':
        # store aggregated result in db
        results_agg_coll = db[cfg.col_aggregated_result]
        result_agg = {
            'task_id': task_id,
            'result': json.dumps(agg_result),
            'step': task["step"]
        }
        results_agg_coll.update_one({'task_id': task_id}, {'$set': result_agg}, upsert=True)

    
    global channel
    channel.queue_declare(queue=cfg.mq_task_scheduler_status)
    channel.basic_publish(exchange="", routing_key=cfg.mq_task_scheduler_status, body=json.dumps(status_update_msg))

address = cfg.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address.ip, address.port))
channel = connection.channel()
channel.queue_declare(queue=cfg.mq_aggregator_form)
channel.basic_consume(queue=cfg.mq_aggregator_form, on_message_callback=callback, auto_ack=True)

print('Form aggregator is listening...')
channel.start_consuming()