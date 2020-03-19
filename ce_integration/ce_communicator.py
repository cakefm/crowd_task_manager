# read messages from ce_messages queue
# make calls to ce api
import time
from datetime import datetime
import requests
import json
import urllib.request
from urllib.parse import urlparse
from werkzeug.utils import secure_filename
import yaml
import pika
import pymongo
import os
from pathlib import Path
import pathlib
from bson.objectid import ObjectId
import sys

with open("../settings.yaml", 'r') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

sys.path.append("..")
import common.settings as settings
import common.file_system_manager as fsm


CE_SERVER = cfg['ce_server']
SERVER_ADDRESS = cfg['current_server']
RABBITMQ_ADDRESS = settings.rabbitmq_address[0]
RABBITMQ_PORT = settings.rabbitmq_address[1]
MONGO_SERVER = cfg['mongo_server']
MONGO_DB = cfg['mongo_db']
ENTRYPOINT_ID = cfg["entrypoint_id"]
PROCESSING_POTENTIALACTION_ID = cfg["processing_potentialaction_id"]
VERIFY_POTENTIALACTION_ID = cfg["verify_potentialaction_id"]
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}
UPLOAD_FOLDER = str(Path.home()) + cfg['upload_folder']


def add_to_queue(queue, routing_key, msg):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_ADDRESS,
            port=RABBITMQ_PORT))
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    print(RABBITMQ_ADDRESS, RABBITMQ_PORT, msg)
    channel.basic_publish(exchange='', routing_key=routing_key, body=msg)
    connection.close()


def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def store_sheet(source):
    a = urlparse(source)
    filename = os.path.basename(a.path)
    name_only = os.path.splitext(filename)[0]
    if allowed_file(filename):
        # prep data location
        filename = secure_filename(filename)
        path_whole_files = os.path.join(UPLOAD_FOLDER, os.path.splitext(filename)[0])
        path_whole_files = os.path.join(path_whole_files, "whole")
        pathlib.Path(path_whole_files).mkdir(parents=True, exist_ok=True)
        sheet_path = os.path.join(path_whole_files, filename)
        # get the data
        print("url retrieve begin")
        urllib.request.urlretrieve(source, sheet_path)
        print("url retrieve end")

        # create entry into database
        myclient = pymongo.MongoClient(MONGO_SERVER)
        mydb = myclient[MONGO_DB]
        mycol = mydb["sheets"]

        result = {
            "name": os.path.splitext(filename)[0],
            "description": "retreived from CE",
            "sheet_path": str(sheet_path),
            "ts": datetime.now()
        }
        entry = mycol.insert_one(result).inserted_id
        return str(entry)


def poll_controlactions():
    url = CE_SERVER
    payload = "{\"query\":\"query {\\n  ControlAction{\\n    identifier\\n    name\\n    actionStatus\\n    url\\n    target {\\n      ... on EntryPoint {\\n        identifier\\n        name\\n      }\\n    }\\n    wasDerivedFrom{\\n      __typename\\n      identifier\\n      \\n    }\\n    object {\\n      __typename\\n      ... on PropertyValue {\\n        name\\n        identifier\\n        valueReference\\n        value\\n        nodeValue {\\n          __typename\\n          ... on DigitalDocument {\\n            name\\n            identifier\\n            source\\n          }\\n        }\\n      }\\n    }\\n  }\\n}\\n\"}"
    headers = headers = {'content-type': 'application/json'}
    response = requests.request("POST", url, data=payload, headers=headers)
    # print(response.text)
    json_object = json.loads(response.text)
    # check of there are any new controlactions
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb["sheets"]
    myquery = {}
    mydoc = mycol.find(myquery)

    known_scores = []
    for score in mydoc:
        known_scores.append(score['name'])

    for action in json_object["data"]["ControlAction"]:
        if len(action["object"]) > 0:
            name = action['object'][0]['nodeValue']['name']
            source = action['object'][0]['nodeValue']['source']
            name_only = os.path.splitext(name)[0]
            # print(name_only)
            # TODO: check if filename is pdf or other allowed format
            if name_only not in known_scores:
                print(name_only, " not currently known")
                identifier = store_sheet(source)
                known_scores.append(name_only)
                message = {'score_name': name, '_id': identifier}
                add_to_queue('omr_planner_queue', 'omr_planner_queue', json.dumps(message))
                # send message to omr planner that there is a new sheet


def create_controlaction(task_id):
    # query task data from db
    myclient = pymongo.MongoClient(MONGO_SERVER)
    mydb = myclient[MONGO_DB]
    mycol = mydb["tasks"]
    query = {'_id': ObjectId(task_id)}
    result = mycol.find_one(query)
    name = result['name']
    description = ''
    task_url = SERVER_ADDRESS + 'edit/' + task_id
    actionStatus = "PotentialActionStatus"

    # submit task data to ce
    url = CE_SERVER
    payload = "{\"query\":\"mutation {\\n   RequestControlAction(\\n       controlAction: {\\n           entryPointIdentifier: \\\"%s\\\"\\n           potentialActionIdentifier: \\\"%s\\\"\\n           potentialAction: {\\n               name: \\\"%s\\\"\\n               url: \\\"%s\\\"\\n           }\\n       }\\n   ) {\\n       identifier\\n   }\\n}\\n\"}"
    payload = payload % (ENTRYPOINT_ID, PROCESSING_POTENTIALACTION_ID, name, task_url)
    headers = headers = {'content-type': 'application/json'}
    response = requests.request("POST", url, data=payload, headers=headers)
    data = json.loads(response.text)

    # record the submitted task to db
    ce_id = data['data']['CreateControlAction']['identifier']
    submitted_task = {
        'ce_identifier': ce_id,
        'task_id': task_id,
        'xml': [],
        'ts': datetime.now(),
        'url': task_url,
        'status': 'pending',
        'name': result['name']
    }
    submitted_task_coll = mydb['submitted_tasks']
    submitted_task_coll.insert_one(submitted_task)


def update_control_action_status(identifier, action_status):
    url = CE_SERVER
    # ActiveActionStatus,
    # CompletedActionStatus,
    # FailedActionStatus,
    # PotentialActionStatus
    payload = "{\"query\":\"mutation{\\n  UpdateControlAction(\\n    identifier: \\\"%s\\\",\\n    actionStatus: %s\\n  )\\n  {\\n    identifier,\\n    actionStatus,\\n    url\\n  }\\n}\"}"
    payload = payload % (identifier, action_status)
    headers = {'content-type': 'application/json'}
    response = requests.request("POST", url, data=payload, headers=headers)
    print(response.text)


def main():
    try:
        begin_time = datetime.now()
        sleep_in_minutes = 1
        while True:
            if((datetime.now() - begin_time).total_seconds() > (sleep_in_minutes * 30)):
                print(datetime.now(), "Monitor Upload")
                poll_controlactions()
                begin_time = datetime.now()
                print(datetime.now(), "Nap Time!")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_ADDRESS,
                    port=RABBITMQ_PORT))
            channel = connection.channel()
            method_frame, header_frame, body = channel.basic_get('ce_communicator_queue')
            if method_frame:
                print(datetime.now(), str(body, 'utf-8'))
                msg = json.loads(body.decode("utf-8"))
                # forward message to ce
                # task created message
                if msg['action'] == 'task created':
                    create_controlaction(
                        msg['_id'])
                # task completed message
                elif msg['action'] == 'task completed':
                    update_control_action_status(
                        msg['identifier'],
                        msg['status'])
                channel.basic_ack(method_frame.delivery_tag)
            channel.close()
            connection.close()
    except KeyboardInterrupt:
        print('interrupted!')


if __name__ == "__main__":
    main()
