import sys
import pika
import json
import numpy as np

sys.path.append("..")
import common.settings as settings
import common.file_system_manager as fsm
import common.tree_alignment as ta
import common.tree_tools as tt
import xml.dom.minidom as xml

from pymongo import MongoClient


def callback(ch, method, properties, body):
    data = json.loads(body)
    # We need: 
    # - name: name of the current sheet 
    # - partials: file names of the additional MEI files (including extension) (list). 
    # These "partial" MEIs need to be in the "whole" folder of the sheet already, just like the skeleton.mei
    sheet_name = data['name']
    partial_file_names = data['partials']

    whole_dir = fsm.get_sheet_whole_directory(sheet_name)
    skeleton_path = whole_dir / 'skeleton.mei'
    partial_paths = [whole_dir / partial for partial in partial_file_names]

    # skeleton always has 1 section which just contains the measures and some additional tags
    skeleton_document = xml.parse(str(skeleton_path)).documentElement
    skeleton_section = skeleton_document.getElementsByTagName("section")[0]
    skeleton_section_xml = tt.purge_non_element_nodes(skeleton_section).toxml() 
    partial_sections_xml = []
    for partial_path in partial_paths:
        partial = xml.parse(str(partial_path))
        # We have to extract the measures and put them under a "fake" section root to get a similar structure as the skeleton
        partial = tt.replace_child_nodes(tt.create_element_node("section"), partial.getElementsByTagName("measure"))
        partial = tt.purge_non_element_nodes(partial)
        partial_sections_xml.append(partial.toxml())

    # Perform the alignments and node picking
    aligned_trees = ta.align_trees_multiple([skeleton_section_xml] + partial_sections_xml)
    final_section_tree, _ = ta.build_consensus_tree(aligned_trees, consensus_method=ta.consensus_bnd_enrich_skeleton)

    # The final tree only aligned the section with measures, so we need to put the contents of that section back now
    tt.replace_child_nodes(skeleton_section, final_section_tree.childNodes)

    # Write the final tree to a file
    with open(whole_dir / 'aligned.mei', 'w') as aligned_mei_file:
        # We also purge everything that is not an element, to keep the tree clean and easily output a prettified XML file
        aligned_mei_file.write(tt.purge_non_element_nodes(skeleton_document).toprettyxml())
    
    # Update task status
    # status_update_msg = {
    # '_id': task_id,
    # 'module': 'aggregator_xml',
    # 'status': 'complete'
    # }

    # For now, only consider the tree to be good enough if consensus was reached for every node
    
    # global channel
    # channel.queue_declare(queue=settings.task_status_queue_name)
    # channel.basic_publish(exchange="", routing_key="aligner", body=json.dumps(status_update_msg))

address = settings.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address[0], address[1]))
channel = connection.channel()
n = "aligner_queue"
channel.queue_declare(queue=n)
channel.basic_consume(queue=n, on_message_callback=callback, auto_ack=True)

print('XML aligner is listening...')
channel.start_consuming()