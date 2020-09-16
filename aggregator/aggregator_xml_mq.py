import sys
import pika
import json
import numpy as np

sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
import common.tree_alignment as ta
import xml.dom.minidom as xml

from pymongo import MongoClient


def callback(ch, method, properties, body):
    data = json.loads(body)
    task_id = data['task_id']

    client = MongoClient(cfg.mongodb_address.ip, int(mongodb_address.port))
    db = client[cfg.db_name]

    results = []

    results_json = db[cfg.col_result].find({
        "task_id": task_id,
        "result_type": "edit"})
    for thing in results_json:
        results.append(thing['xml'])

    # results = db[cfg.col_result].find({"task_id" : task_id})

    aligned_trees = ta.align_trees_multiple(results)

    final_tree, consensus_per_node = ta.build_consensus_tree(aligned_trees)

    # print(final_tree.toprettyxml())
    # Update task status
    status_update_msg = {
        '_id': task_id,
        'module': 'aggregator_xml',
        'status': 'complete'
    }

    # For now, only consider the tree to be good enough if consensus was reached for every node
    if sum(consensus_per_node.values()) < len(consensus_per_node):
        status_update_msg['status'] = 'failed'
    else:
        # store aggregated result in db
        results_agg_coll = db[cfg.col_aggregated_result]
        result_agg = {
            'task_id': task_id,
            'xml': str(final_tree.toprettyxml())
        }
        results_agg_coll.update_one({'task_id': task_id}, {'$set': result_agg}, True)

    global channel
    channel.queue_declare(queue=cfg.mq_task_status)
    channel.basic_publish(exchange="", routing_key=cfg.mq_task_status, body=json.dumps(status_update_msg))

address = cfg.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address.ip, address.port))
channel = connection.channel()
channel.queue_declare(queue=cfg.mq_aggregator_xml)
channel.basic_consume(queue=cfg.mq_aggregator_xml, on_message_callback=callback, auto_ack=True)

print('XML aggregator is listening...')
channel.start_consuming()