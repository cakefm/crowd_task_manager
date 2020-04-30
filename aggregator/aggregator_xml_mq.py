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
    
    # Get the cumulative distances of all the nodes to one another
    node_distance_bins = [0] * len(nodes)
    for i in range(len(nodes)):
        for j in range(len(nodes)): 
            node_distance_bins[i] += node_distances[i, j]
            node_distance_bins[j] += node_distances[i, j]

    # Idea for threshold check: see how many nodes are within 1 std of best node regarding cumulative distance
    # Then get the ratio between this and the total amount of candidates
    # This number will get close to 1 if many of the nodes agree with the best node
    std = np.std(node_distance_bins)
    mcd = min(node_distance_bins)
    candidates_that_agree = [x for x in node_distance_bins if x > mcd - std and x < mcd + std]
    ratio = len(candidates_that_agree) / len(nodes)
    consensus = False
    if ratio > settings.aggregator_xml_threshold:
        consensus = True

    return nodes[np.argmin(node_distance_bins)], consensus


def build_consensus_tree(trees, consensus_method = best_node_distance, exclude = []):
    consensus_per_node = {}
    return _build_consensus_tree(trees, ta.create_gap_element(), 0, consensus_method, exclude, node_consensus_dict).childNodes[0], consensus_per_node


def _build_consensus_tree(trees, new_tree, n, consensus_method, exclude, node_consensus_dict):
    group = zip(*[c.childNodes for c in trees])
    if group:
        for nodes in group:
            best, consensus = consensus_method(nodes)
            node_consensus_dict[best] = consensus
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

    final_tree, consensus_per_node = build_consensus_tree(aligned_trees, exclude = [ta.GAP_ELEMENT_NAME])

    # Update task status
    status_update_msg = {
    '_id': task_id,
    'module': 'aggregator_xml',
    'status': 'complete'
    }

    # For now, only consider the tree to be good enough if consensus was reached for every node
    if sum(consensus_per_node.values()) < len(consensus_per_node):
        status_update_msg['status'] = 'failed'
    
    global channel
    channel.queue_declare(queue=settings.task_status_queue_name)
    channel.basic_publish(exchange="", routing_key=settings.task_status_queue_name, body=json.dumps(status_update_msg))

address = settings.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address[0], address[1]))
channel = connection.channel()
channel.queue_declare(queue=settings.aggregator_xml_queue_name)
channel.basic_consume(queue=settings.aggregator_xml_queue_name, on_message_callback=callback, auto_ack=True)

print('XML aggregator is listening...')
channel.start_consuming()