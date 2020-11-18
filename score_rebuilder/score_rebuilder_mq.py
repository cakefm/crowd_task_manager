import sys
import pika
import json

sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
import common.tree_tools as tt
import xml.dom.minidom as xml

from pymongo import MongoClient

# TODO: It's better to make the score rebuilder "stupid", as in, it should rebuild 
# the entire score from scratch from the data in the db. This way it can be triggered in a
# rather generic way, at any point in the pipeline
def callback(ch, method, properties, body):
    data = json.loads(body)
    sheet_name = data['name']
    task_id = data['task_id']

    client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
    db = client[cfg.db_name]

    # Obtain aggregated XML
    aggregated_result = db[cfg.col_aggregated_result].find_one({"task_id" : task_id})
    aggregated_xml = xml.parseString("<mei>" + aggregated_result["xml"] + "</mei>")
    aggregated_dict = {x.attributes["n"].value:x for x in aggregated_xml.getElementsByTagName("measure")}

    # Get MEI file and measures
    mei_path = fsm.get_sheet_whole_directory(sheet_name) / "aligned.mei"
    mei_xml = xml.parse(str(mei_path))
    mei_measures = mei_xml.getElementsByTagName("measure")

    # Replace measures with new info
    for measure in mei_measures:
        mei_n = measure.attributes["n"].value
        if mei_n in aggregated_dict:
            tt.replace_child_nodes(measure, aggregated_dict[mei_n].childNodes)

    # Write MEI file
    with open(str(mei_path), 'w') as mei_file:
        mei_file.write(tt.purge_non_element_nodes(mei_xml.documentElement).toprettyxml())

    status_update_msg = {
    '_id': task_id,
    'module': 'score_rebuilder',
    'status': 'complete'
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