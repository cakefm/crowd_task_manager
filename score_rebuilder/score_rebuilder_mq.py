import sys
import pika
import json

sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
import common.tree_tools as tt
import common.tree_alignment as ta

import xml.dom.minidom as xml
from bson.objectid import ObjectId

from pymongo import MongoClient


def callback(ch, method, properties, body):
    data = json.loads(body)
    sheet_name = data['name']
    task_id = data['task_id']

    client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
    db = client[cfg.db_name]

    # Get MEI file and measures
    mei_path = fsm.get_sheet_whole_directory(sheet_name) / "aligned.mei"
    mei_xml_tree = xml.parse(str(mei_path))
    # mei_measures = mei_xml.getElementsByTagName("measure")

    # Obtain corresponding task and slice
    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})
    # measure_staff_slice = db[cfg.col_slice].find_one({"_id" : ObjectId(task["slice_id"])})
    # slice_measures = mei_measures[measure_staff_slice["start"]: measure_staff_slice["end"]]

    # Get aggregated XML
    aggregated_result = db[cfg.col_aggregated_result].find_one({"task_id": task_id, "step": task["step"]})

    if aggregated_result:
        aggregated_xml = aggregated_result["result"]

        # Temporary solution: give the slice somewhat more context by inserting only the header of the previous measure into it
        tree = xml.parseString(aggregated_xml).documentElement
        index = int(tree.getElementsByTagName("measure")[0].getAttribute("n")) - 1  # n-index is shifted up by 1
        if index > 0:
            measure = mei_xml_tree.getElementsByTagName("measure")[index - 1].cloneNode(deep=True)  # get the previous measure
            measure.childNodes = []
            tree.insertBefore(measure, tree.childNodes[0])
            aggregated_xml = tree.toxml()

        # Perform combination with original MEI via tree aligner
        mei_section = mei_xml_tree.getElementsByTagName("section")[0]
        mei_section_xml = mei_section.toxml()
        aligned_trees = ta.align_trees_multiple([mei_section_xml, aggregated_xml], distance_function=ta.node_distance_anchored)
        final_section_tree, _ = ta.build_consensus_tree(aligned_trees, consensus_method=ta.consensus_bnd_override_inner)
        tt.replace_child_nodes(mei_section, final_section_tree.childNodes)

        # Write MEI file
        with open(str(mei_path), 'w') as mei_file:
            mei_file.write(tt.purge_non_element_nodes(mei_xml_tree.documentElement).toprettyxml())

        status_update_msg = {
            '_id': task_id,
            'module': 'score_rebuilder',
            'status': 'complete'
        }
    else:
        print(f"Aggregated result for task with id {task_id} at step {task['step']} did not exist!")
        status_update_msg = {
            '_id': task_id,
            'module': 'score_rebuilder',
            'status': 'failed'
        }

    global channel
    channel.queue_declare(queue=cfg.mq_task_scheduler_status)
    channel.basic_publish(exchange="", routing_key=cfg.mq_task_scheduler_status, body=json.dumps(status_update_msg))


address = cfg.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address.ip, address.port))
channel = connection.channel()
channel.queue_declare(queue=cfg.mq_score_rebuilder)
channel.basic_consume(queue=cfg.mq_score_rebuilder, on_message_callback=callback, auto_ack=True)

print('Score rebuilder is listening...')
channel.start_consuming()
