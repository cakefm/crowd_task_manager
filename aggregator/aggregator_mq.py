import sys
import pika
import json
import numpy as np

sys.path.append("..")
import common.settings as settings
import common.file_system_manager as fsm
import common.tree_alignment as ta
import xml.dom.minidom as xml

from pymongo import MongoClient


def best_node_distance(nodes):
    node_distances = np.full((len(nodes), len(nodes)), np.inf)
    for i, a in enumerate(nodes):
        for j, b in enumerate(nodes): 
            node_distances[i, j] = ta.node_distance(a, b)
    
    node_distance_bins = [0] * len(nodes)
    for i in range(len(nodes)):
        for j in range(len(nodes)): 
            node_distance_bins[i] += node_distances[i, j]
            node_distance_bins[j] += node_distances[i, j]

    return nodes[np.argmin(node_distance_bins)]


def build_consensus_tree(trees, consensus_method = best_node_distance, exclude = []):
    return _build_consensus_tree(trees, ta.create_gap_element(), 0, consensus_method, exclude).childNodes[0]


def _build_consensus_tree(trees, new_tree, n, consensus_method, exclude):
    group = zip(*[c.childNodes for c in trees])
    if group:
        for nodes in group:
            best = consensus_method(nodes)
            if best.tagName in exclude:
                continue

            new_node =  ta.create_gap_element()
            new_node.tagName = best.tagName
            for key in best.attributes.keys():
                new_node.setAttribute(key, best.attributes[key].value)
            new_tree.childNodes.append(new_node)
            _build_consensus_tree(nodes, new_node, n + 1, consensus_method, exclude)
    return new_tree


def callback(ch, method, properties, body):
    data = json.loads(body)
    task_id = data['task_id']

    client = MongoClient(settings.mongo_address[0], int(settings.mongo_address[1]))
    db = client.trompa_test

    results = db[settings.result_collection_name].find_one({"task_id" : task_id})["results"]

    aligned_trees = ta.align_trees_multiple(results)

    final_tree = build_consensus_tree(aligned_trees, exclude = [ta.GAP_ELEMENT_NAME])
    
    
    print()
    print()
    print("==== FINAL TREE ====")
    print(final_tree.childNodes[0].toprettyxml())
    print()


       

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