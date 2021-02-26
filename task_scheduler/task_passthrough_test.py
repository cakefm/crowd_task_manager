import sys
sys.path.append("..")
from common.settings import cfg
import common.tree_tools as tt
import common.file_system_manager as fsm
import time
import requests
import pika
import json
import numpy.random as rnd
from bson.objectid import ObjectId
import xml.dom.minidom as xml
import uuid

from pymongo import MongoClient

# Choices
key_modes = ["minor", "major"]
key_sigs = ["3f", "2f", "1f"]
meter_units = ["1", "2", "3", "4"]
meter_counts = ["1", "2", "3", "4"]

# <note dur="4" oct="4" pname="a"/>
note_dur = ["4", "8"]
note_oct = ["4", "5", "6"]
note_pname = ["a", "b", "c", "d", "e", "f", "g"]

# Fractions
scoredef_chance = 0.8
clef_chance = 0.1
note_chance = 0.1

# Which staff/measure indices to process
indices = {}
staff_indices = {}

def populate_payload_xml(score, tree, task):
    task_type_name = task["type"]
    task_step_name = task["step"]
    measures = tree.getElementsByTagName("measure")
    first_measure_index = measures[0].getAttribute("n")

    # Clef recognition
    if task_type_name=="0_clef_recognition" and task_step_name=="edit" and clef_chance > rnd.random():
        node = tt.create_element_node("clef", {"shape":"G", "line":"2"})
        staff = tree.getElementsByTagName("staff")[0]
        first_staff_index = staff.getAttribute('n')
        print(f"  Putting clef {node.toxml()} in measure n={first_measure_index}, staff n={first_staff_index}")

        for layer in staff.getElementsByTagName("layer"):
            layer.appendChild(node.cloneNode(deep=True))
            break

    # Key recognition
    elif task_type_name=="1_time_recognition" and task_step_name=="edit" and scoredef_chance > rnd.random():
        node = tt.create_element_node("scoreDef", {"meter.count": rnd.choice(meter_counts), "meter.unit": rnd.choice(meter_units), "crmp_id": str(uuid.uuid4())})
        print(f"  Putting scoreDef {node.toxml()} before measure n={first_measure_index}")
        tree.insertBefore(node, measures[0]).parentNode

    # Time recognition
    elif task_type_name=="2_key_recognition" and task_step_name=="edit" and scoredef_chance > rnd.random():
        node = tt.create_element_node("scoreDef", {"key.sig": rnd.choice(key_sigs), "key.mode": rnd.choice(key_modes), "crmp_id": str(uuid.uuid4())})
        print(f"  Putting scoreDef {node.toxml()} before measure n={first_measure_index}")
        tree.insertBefore(node, measures[0]).parentNode

    return tt.purge_non_element_nodes(tree).toxml()


def populate_payload_form(score, tree, task, data):
    task_type_name = task["type"]
    task_step_name = task["step"]

    if task_type_name=="1_detect_clefs" and task_step_name=="verify":
        data["verify"] = [rnd.random() < 0.8]

    return json.dumps(data)

def callback(channel, method, properties, body):

    message = json.loads(body)
    task_id = message["task_id"]
    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})
    score = db[cfg.col_score].find_one({"name": task["score"]})
    task_type_name = task["type"]
    task_type = db[cfg.col_task_type].find_one({"name": task_type_name})
    task_step_name = task["step"]
    threshold = task_type["steps"][task_step_name]["min_responses"]
    result_type = task_type["steps"][task_step_name]["result_type"]

    tree = xml.parseString(task['xml']).documentElement

    print(f"Processing task {task_id} of type {task_type} at step {task_step_name}; providing result type {result_type}")

    if result_type=="xml":
        payload = populate_payload_xml(score, tree, task)
    elif result_type=="form":
        payload = populate_payload_form(score, tree, task, {})

    # TODO: Somehow vary the responses per submission
    for i in range(threshold):
        requests.post(f"http://localhost:8888/{task['_id']}", data=payload)
        for j in range(0): # Putting this to >0 will simulate delay in response
            connection.process_data_events()
            time.sleep(0.2)

    print(f"  Passed through task with ID {task['_id']} of type:step {task['type']}:{task['step']} as result {threshold} times")
    print(f"  - payload: {payload}")


if __name__ == "__main__":
    processed = set()
    client = MongoClient(*cfg.mongodb_address)
    db = client[cfg.db_name]

    # Pika
    parameters = pika.ConnectionParameters(*cfg.rabbitmq_address)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=cfg.mq_task_passthrough)
    channel.basic_consume(
        on_message_callback=callback, 
        queue=cfg.mq_task_passthrough, 
        auto_ack=True
    )

    print('Task passthrough is listening...')
    channel.start_consuming()