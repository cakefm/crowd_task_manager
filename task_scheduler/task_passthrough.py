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

error_chance = 0.0

task_set = set()



def set_attributes(node, attributes):
    for attr in attributes:
        node.setAttribute(attr, attributes[attr])

def randomize_clef(tree):
    clef = tt.create_element_node("clef")
    choice = rnd.choice([
        {"shape": "G", "line": "2"},
        {"shape": "F", "line": "4"},
        {"shape": "C", "line": "3"},
        {}
    ])
    set_attributes(clef, choice)
    tt.first_or_none(tree, "layer").appendChild(clef)
    return tree.toxml()

def randomize_key(tree):
    measure = tt.first_or_none(tree, "measure")
    n = tt.first_or_none(tree, "staff").getAttribute("n")
    scoreDef = tt.first_or_none(tree, "scoreDef")
    if not scoreDef:
        scoreDef = tt.create_element_node("scoreDef")
        tree.insertBefore(scoreDef, measure)

    staffGrp = tt.first_or_none(scoreDef, "staffGrp")
    if not staffGrp:
        staffGrp = tt.create_element_node("staffGrp")
        scoreDef.appendChild(staffGrp)

    staffDef = tt.first_or_none(staffGrp, "staffDef", lambda e: e.getAttribute("n") == n)
    if not staffDef:
        staffDef = tt.create_element_node("staffDef", {"n": n})
        staffGrp.appendChild(staffDef)
    
    choice = rnd.choice([
        {"key.sig.show": "true", "key.sig": f"{rnd.randint(5)}f"},
        {"key.sig.show": "true", "key.sig": f"{rnd.randint(5)}s"},
        {"key.sig.show": "true", "key.sig": f"{rnd.randint(5)}0"},
        {}
    ])

    set_attributes(staffDef, choice)

    return tree.toxml()


def randomize_time(tree):
    measure = tt.first_or_none(tree, "measure")
    n = tt.first_or_none(tree, "staff").getAttribute("n")
    scoreDef = tt.first_or_none(tree, "scoreDef")
    if not scoreDef:
        scoreDef = tt.create_element_node("scoreDef")
        tree.insertBefore(scoreDef, measure)

    staffGrp = tt.first_or_none(scoreDef, "staffGrp")
    if not staffGrp:
        staffGrp = tt.create_element_node("staffGrp")
        scoreDef.appendChild(staffGrp)

    staffDef = tt.first_or_none(staffGrp, "staffDef", lambda e: e.getAttribute("n") == n)
    if not staffDef:
        staffDef = tt.create_element_node("staffDef", {"n": n})
        staffGrp.appendChild(staffDef)
    
    choice = rnd.choice([
        {"meter.sym": "true"},
        {"meter.count": f"{1 + rnd.randint(4)}", "meter.unit": f"{1 + rnd.randint(4)}"},
        {}
    ])

    set_attributes(staffDef, choice)

    return tree.toxml() 

def randomize_note(tree):
    note_count = rnd.randint(8)
    layer = tt.first_or_none(tree, "layer")
    for i in range(note_count):
        element = tt.create_element_node(
            rnd.choice(["note", "rest"]), 
            {"dur": rnd.choice(['1/32', '1/16', '1/8', '1/4', '1/2', '1'])}
        )
        layer.appendChild(element)
    

    return tree.toxml()

def randomize_pitch(tree):
    notes = tree.getElementsByTagName("note")

    for note in notes:
        set_attributes(note, 
            {
            "pname":f"{rnd.choice(['a', 'b', 'c', 'd', 'e', 'f', 'g'])}", 
            "oct":f"{3 + rnd.randint(3)}"
            }
        )

    return tree.toxml()


def callback(channel, method, properties, body):
    message = json.loads(body)
    if 'action' in message and message['action'] != 'task created':
        print(f"Ignoring message {message}...")
        return
    task_id = message["task_id"]

    if task_id in task_set:
        print(f"[Warning] Got an additional creation request for task {task_id}")
    task_set.add(task_id)

    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})
    task_type_name = message['task_type']
    
    # score = db[cfg.col_score].find_one({"name": task["score"]})
    task_type = db[cfg.col_task_type].find_one({"name": task_type_name})
    task_step_name = task["step"]

    if task_step_name == "DONE":
        print("[Warning] Received creation request for a task that was done already, skipping...")
        return

    threshold = task_type["steps"][task_step_name]["min_responses"]
    # result_type = task_type["steps"][task_step_name]["result_type"]

    tree = xml.parseString(task['xml']).documentElement

    payload_func = {
        "0_clef_recognition": randomize_clef,
        "1_time_recognition": randomize_time,
        "2_key_recognition": randomize_key,
        "3_note_transcription": randomize_note,
        "4_note_pitch": randomize_pitch
    }[task_type_name]

    payload = payload_func(tree)

    # print(f"Processing task {task_id} of type {task_type} at step {task_step_name}; providing result type {result_type}")

    # if result_type=="xml":
    #     payload = populate_payload_xml(score, tree, task)
    # elif result_type=="form":
    #     payload = populate_payload_form(score, tree, task, {})

    for i in range(threshold):
        if (error_chance > rnd.random()):
            payload = payload_func(tree)
        requests.post(f"http://localhost:443/{task['_id']}", data=payload)
        print(f"Sending response to API for task {task_id} (count {1 + i})")
        for j in range(2): # Putting this to >0 will simulate delay in response
            connection.process_data_events()
            time.sleep(0.1 + 0.1 * rnd.random())

    # print(f"  Passed through task with ID {task['_id']} of type:step {task['type']}:{task['step']} as result {threshold} times")
    # print(f"  - payload: {payload}")


if __name__ == "__main__":
    processed = set()
    client = MongoClient(*cfg.mongodb_address)
    db = client[cfg.db_name]

    # Pika
    parameters = pika.ConnectionParameters(*cfg.rabbitmq_address)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=cfg.mq_ce_communicator)
    channel.basic_consume(
        on_message_callback=callback, 
        queue=cfg.mq_ce_communicator, 
        auto_ack=True
    )

    print('Task passthrough is listening...')
    channel.start_consuming()