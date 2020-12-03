import sys
import pika
import json

sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
import common.tree_tools as tt
import xml.dom.minidom as xml
from bson.objectid import ObjectId

from pymongo import MongoClient

# Score rebuilder should always re-index, and rely on the measure order instead of the n./label-attribute
def callback(ch, method, properties, body):
    data = json.loads(body)
    sheet_name = data['name']
    task_id = data['task_id']

    client = MongoClient(*cfg.mongodb_address)
    db = client[cfg.db_name]

    # Obtain corresponding task and slice
    task = db[cfg.col_task].find_one({"_id" : ObjectId(task_id)})

    # Get aggregated XML
    aggregated_result = db[cfg.col_aggregated_result].find_one({
        "task_id" : task_id,
        "step" : task["step"]
    })

    status_update_msg = {
        "_id": task_id,
        "module": "form_processor"
    }

    form_output = json.loads(aggregated_result["result"])
    # General procedure for all verification steps
    if task["step"]=="verify":
        verification_passed = form_output["verify"][0]
        if verification_passed:
            status_update_msg["status"] = "verification-passed"
        else:
            status_update_msg["status"] = "verification-failed"

    if "status" not in status_update_msg:
        raise Exception(f"Task of type {task['type']} did not receive a status, make sure it gets handled in this module!")

    global channel
    channel.queue_declare(queue=cfg.mq_task_scheduler_status)
    channel.basic_publish(exchange="", routing_key=cfg.mq_task_scheduler_status, body=json.dumps(status_update_msg))


connection = pika.BlockingConnection(pika.ConnectionParameters(*cfg.rabbitmq_address))
channel = connection.channel()
channel.queue_declare(queue=cfg.mq_form_processor)
channel.basic_consume(queue=cfg.mq_form_processor, on_message_callback=callback, auto_ack=True)

print('Score rebuilder is listening...')
channel.start_consuming()