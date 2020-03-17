import sys
import pika
import json

sys.path.append("..")
import common.settings as settings
import common.file_system_manager as fsm
import xml.dom.minidom as xml

from pymongo import MongoClient


def callback(ch, method, properties, body):
    data = json.loads(body)
    sheet_name = data['name']
    task_id = data['task_id']

    client = MongoClient(settings.mongo_address[0], int(settings.mongo_address[1]))
    db = client.trompa_test
    sheet_id = str(db[settings.sheet_collection_name].find_one({"name" : sheet_name})["_id"])

    # Obtain aggregated XML
    aggregated_result = db[settings.aggregated_result_collection_name].find_one({"task_id" : task_id})
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
            measure.childNodes = aggregated_dict[mei_n].childNodes

    # Write MEI file
    with open(str(mei_path), 'w') as mei_file:
        mei_file.write(mei_xml.toxml())

    status_update_msg = {
    '_id': sheet_id,
    'module': 'score_rebuilder',
    'status': 'complete',
    'name': sheet_name,
    'task_id': task_id
    }

    global channel
    channel.queue_declare(queue="status_queue")
    channel.basic_publish(exchange="", routing_key="status_queue", body=json.dumps(status_update_msg))


address = settings.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address[0], address[1]))
channel = connection.channel()
channel.queue_declare(queue=settings.score_rebuilder_queue_name)
channel.basic_consume(queue=settings.score_rebuilder_queue_name, on_message_callback=callback, auto_ack=True)

print('Score rebuilder is listening...')
channel.start_consuming()