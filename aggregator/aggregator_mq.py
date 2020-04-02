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


    # Use the pairwise alignments with a heuristic to come up with a multiple alignment solution
    mas = []
    candidate_pairs = sorted([(i, j) for i, j in pairs if main_tree_index in (i, j)], key = lambda t : distances[t[0], t[1]])
    closest_pair_index = candidate_pairs[0]
    closest_pair = pairs[closest_pair_index]
    mas.append(closest_pair[closest_pair_index.index(main_tree_index)])
    mas.append(closest_pair[not closest_pair_index.index(main_tree_index)])
    for pair_index in candidate_pairs[1:]:
        pair = pairs[pair_index]
        main_tree = pair[pair_index.index(main_tree_index)]
        cand_tree = pair[not pair_index.index(main_tree_index)]

        print(f"ITERATION {candidate_pairs.index(pair_index)}:")
        print("===MAIN:")
        print(main_tree.toprettyxml())
        print("===CAND:")
        print(cand_tree.toprettyxml())
        print()

        # Copy the gaps
        for tree in mas:
            tad.copy_gaps(main_tree, tree)

        mas.append(cand_tree)

        print("===RESULT SO FAR:")
        for tree in mas:
            print(tree.toprettyxml())
            print("============")

        print()
        print()
        print()
        print("------------------------------------------------")


    # At this point all trees in `mas` have the same structure, thus we can iterate
    # over the nodes and determine node values through consensus

    def find_best_node(nodes):
        node_distances = np.full((len(nodes), len(nodes)), np.inf)
        for i, a in enumerate(nodes):
            for j, b in enumerate(nodes): 
                node_distances[i, j] = tad.node_distance(a, b)
        
        node_distance_bins = [0] * len(nodes)
        for i in range(len(nodes)):
            for j in range(len(nodes)): 
                node_distance_bins[i] += node_distances[i, j]
                node_distance_bins[j] += node_distances[i, j]

        return nodes[np.argmin(node_distance_bins)]


    def build_tree(trees, new_tree, n):
        group = zip(*[c.childNodes for c in trees])
        if group:
            for nodes in group:
                best = find_best_node(nodes)
                if best.tagName == 'gap':
                    continue
 
                new_node =  tad.create_gap_element()
                new_node.tagName = best.tagName
                for key in best.attributes.keys():
                    new_node.setAttribute(key, best.attributes[key].value)
                new_tree.childNodes.append(new_node)
                build_tree(nodes, new_node , n + 1)


    consensus_tree = tad.create_gap_element()
    build_tree(mas, consensus_tree, 0)
    # 
    print()
    print()
    print("==== FINAL TREE ====")
    print(consensus_tree.childNodes[0].toprettyxml())
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