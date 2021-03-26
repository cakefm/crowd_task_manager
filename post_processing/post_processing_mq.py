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
    post_processing_steps = data["steps"]
    task_id = data['task_id']

    # Get MEI file
    mei_path = fsm.get_sheet_whole_directory(sheet_name) / "aligned.mei"
    mei_xml_tree = tt.purge_non_element_nodes(xml.parse(str(mei_path)))
    mei_section = mei_xml_tree.getElementsByTagName("section")[0]

    if "clef" in post_processing_steps:
        print(f"Performing clef post-processing for sheet {sheet_name}")
        for layer in mei_xml_tree.getElementsByTagName("layer"):
            element = layer.firstChild

            if element != None and element.tagName=="clef":
                staff = layer.parentNode
                measure = staff.parentNode

                clef_line  = element.getAttribute("line")
                clef_shape = element.getAttribute("shape")
                layer.removeChild(element)

                prev = measure.previousSibling
                scoreDef = None

                while prev:
                    if prev.tagName == "measure":
                        break
                    if prev.tagName == "scoreDef":
                        scoreDef = prev
                        break
                    prev = prev.previousSibling

                # TODO: actually generalize this code
                if not scoreDef:
                    scoreDef = tt.create_element_node("scoreDef")
                    mei_section.insertBefore(scoreDef, measure)

                staffGrp = tt.first_or_none(scoreDef, "staffGrp")
                if not staffGrp:
                    staffGrp = tt.create_element_node("staffGrp")
                    scoreDef.appendChild(staffGrp)

                staffDef = tt.first_or_none(staffGrp, "staffDef", lambda e: e.getAttribute("n") == staff.getAttribute("n"))
                if not staffDef:
                    staffDef = tt.create_element_node("staffDef", {"n": staff.getAttribute("n")})
                    staffGrp.appendChild(staffDef)

                staffDef.setAttribute("clef.line", clef_line)
                staffDef.setAttribute("clef.shape", clef_shape)

    # Write MEI file if there were changes
    if post_processing_steps:
        with open(str(mei_path), 'w') as mei_file:
            mei_file.write(tt.purge_non_element_nodes(mei_xml_tree.documentElement).toprettyxml())

    status_update_msg = {
        '_id': task_id,
        'module': 'post_processing',
        'status': 'complete'
    }

    global channel
    channel.queue_declare(queue=cfg.mq_task_scheduler_status)
    channel.basic_publish(exchange="", routing_key=cfg.mq_task_scheduler_status, body=json.dumps(status_update_msg))


address = cfg.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address.ip, address.port))
channel = connection.channel()
channel.queue_declare(queue=cfg.mq_post_processing)
channel.basic_consume(queue=cfg.mq_post_processing, on_message_callback=callback, auto_ack=True)


client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
db = client[cfg.db_name]

print('Post processor is listening...')
channel.start_consuming()