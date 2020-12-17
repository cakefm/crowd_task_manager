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
from common.settings import cfg
import common.file_system_manager as fsm


CE_SERVER = cfg.ce_server
SERVER_ADDRESS = cfg.current_server
RABBITMQ_ADDRESS = cfg.rabbitmq_address.ip
RABBITMQ_PORT = cfg.rabbitmq_address.port
MONGO_SERVER = cfg.mongodb_address
MONGO_DB = cfg.db_name
ENTRYPOINT_ID = cfg.entrypoint_id
PROCESSING_POTENTIALACTION_ID = cfg.processing_potentialaction_id
VERIFY_POTENTIALACTION_ID = cfg.verify_potentialaction_id
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}
UPLOAD_FOLDER = str(Path.home() / cfg.upload_folder)

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


def store_sheet(source, potentialActionIdentifier, DigitalDocument_id, ControlAction_id):
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
        myclient = pymongo.MongoClient(MONGO_SERVER.ip, MONGO_SERVER.port)
        mydb = myclient[MONGO_DB]
        mycol = mydb[cfg.col_sheet]

        result = {
            "name": os.path.splitext(filename)[0],
            "description": "retreived from CE",
            "sheet_path": str(sheet_path),
            "ts": datetime.now(),
            "source": "CE",
            "edit_action": potentialActionIdentifier,
            "verify_action": potentialActionIdentifier,
            "digitaldocument_id": DigitalDocument_id,
            "controlaction_id": ControlAction_id
        }
        entry = mycol.insert_one(result).inserted_id
        return str(entry)

def poll_controlactions():
    print(datetime.now(), "polling controlActions")
    url = CE_SERVER

    payload = "{\"query\":\"query {\\n  ControlAction(\\n    filter: {\\n      wasDerivedFrom: { \\n        identifier: \\\"b559c52d-6104-4cb3-ab82-39b82bb2de6c\\\"\\n      }\\n    }\\n    actionStatus: PotentialActionStatus\\n  )\\n    {\\n      identifier\\n      name\\n      actionStatus\\n      object {\\n      __typename\\n      ... on PropertyValue {\\n        name\\n        identifier\\n        valueReference\\n        value\\n        nodeValue {\\n          __typename\\n          ... on DigitalDocument {\\n            name\\n            identifier\\n            source\\n          }\\n        }\\n      }\\n    }\\n    } \\n}\"}"
    headers = {'Content-Type': 'application/json'}
    response = requests.request("POST", url, data=payload, headers=headers)

    # print(response.text)
    json_object = json.loads(response.text)
    # check of there are any new controlactions
    myclient = pymongo.MongoClient(MONGO_SERVER.ip, MONGO_SERVER.port)
    mydb = myclient[MONGO_DB]
    mycol = mydb[cfg.col_sheet]
    myquery = {"controlaction_id": {"$exists": True}}
    mydoc = mycol.find(myquery)

    known_campaigns = []
    blacklist = ['2744d89c-163d-4081-8dc1-0fcaee2972f5','db568c1a-d213-4e88-9546-1ecb776bacc4','d0ca5f40-6cc7-49cb-9d88-8c2257987d8b','69904b30-81fc-4506-8ef3-04f480d33762','c57426e6-a995-4703-af55-4adb0f454a9d','f9530dd8-80dc-4875-872e-612c230eb038','60fbf663-2403-4023-98ac-18ebbc8c84e3','393b2764-76aa-4d48-9c25-fb8664d12d21','15c99b6b-6366-404f-8892-2650bea1d5bb','1807dca1-653a-4151-ad88-483a29e879b0','3d1cf563-4523-4a60-84c6-a429e88a43ac','ba4314b7-2f47-4d6f-81d1-8bafb8b8f22c','ddefaa59-ddc9-4089-94fc-13a7ed11a4ef','08f64d9f-7030-4ebd-80f0-a4dabe2df658','e1699130-4507-4796-8ffb-aa2b46770576','d529d2d5-7556-4833-b40f-0ba39ca96878','83f0da90-3f8e-4518-aeac-ab3547b88bc3','45568fb3-e55c-48d1-9103-fc6cb33d8b04','19d40370-c0a4-4f54-b1b8-c0aa16b02f7c','bd48ac24-addc-4a9c-9116-912da7a3de7a','f1461b8a-7f5a-44a1-8cae-21ffa0148c0b','73360322-9273-4253-b485-52775831968b','3a260f17-95a4-42b7-8f72-6ae6553de370','f94f9bbb-894b-4128-bfed-134bced15ea6','a6d33f0c-7040-4268-bce1-03d7558d90fd','3b2c3208-dea5-43f9-9f5f-78fa82932645','3ec6f2d2-0022-49cb-ac86-5bc633521a2d','a5a6d7d1-7661-4af4-9b95-f4527ce6c0b2','96368526-a8cb-4a32-b1cc-0b6fdd064b74','e0429016-3c79-4b84-b5cb-f993a7683da7','b2788265-97ab-4af9-a0a1-2792323995b0','34eb208b-ed13-4854-9f6f-cbed0c9273a6','bd501c5e-63a7-4510-9945-ae55851fc429','3684478d-3780-498a-a747-7df10be1920f','9177d5ab-b495-497f-9d99-3a955e2d19d8','4922eef8-518d-4387-93a2-2d9be1f0d608','fbb85243-0a48-4ca4-a42c-a72e161f1420','54612ff7-45d1-4668-a725-d0d91017f4a0','220de35e-6846-411b-8b5a-a5fd47ebf5fa','d0f1d597-b731-42b8-ab95-6ad3fbde81c3','cf05656f-ee99-4548-b842-dd05c1961513','d609e35c-8881-40c3-a06a-61f9b54b2e2f','8781404d-696d-4ddf-9a72-fd1d181a058f','11e6c778-3c49-41ae-8326-ef51241c5da4','ff78c72c-d322-4ec2-8ef9-440a3730ad54','a29a1568-bbcf-49c6-aaaf-731716d8a31f','789c2766-9573-4294-a59a-9db733d68d1d','8f3a9606-9582-405f-9604-1515a347fac4','f2f34d46-aca4-48d3-95ac-53393c713c40','a45fc911-0b48-44d1-92c8-f34dbfb84c4c','c3e41df2-9c1c-496b-b3d0-3e604623a221','51ed878d-68d0-43d6-bbc5-b8265073d2ed','341330cc-5c22-4ee0-b60d-d445ada244ab','ac991c06-343c-4458-bdc6-a00b9e34565f','2cc144d7-c90a-4d48-ba9e-93416ea63e12','44e6694b-0061-4fb4-a6a0-d6f515c913fc','d78b448a-3124-4915-ae33-9db0cd191128','a552fd2d-9256-49ef-9b42-c6c459d5d20d','15aac7ea-be9a-4431-952a-8dd3124baab3','77b58bec-7359-4644-b58e-eda2b34cbbb7','b08282d5-7f8a-460b-8112-0e910ee2760d','ad47d962-4abe-4410-9a00-36b6d2589343','8572c8d8-d2a1-4167-81ee-6891fa55f69b','047af7c1-4681-4204-92c6-89f8ebb08848','9f81b7f3-8142-4e12-8a92-03154ec9094c','9262ba62-7ccf-4a7b-a4da-3060a34d785a','e23fc45c-9112-4637-a65b-d34232cff6eb','bcfd3aa4-44ac-4e09-97b6-4451a84fc65a']
    for score in mydoc:
        known_campaigns.append(score['controlaction_id'])

    for action in json_object["data"]["ControlAction"]:
        if (len(action["object"]) > 0) and (action['actionStatus'] != "FailedActionStatus"):
            node_nr = 0 if (action['object'][0]['name'] == "Work") else 1
            node_nr_action = 0 if (node_nr == 1) else 1
            name = action['object'][node_nr]['nodeValue']['name']
            source = action['object'][node_nr]['nodeValue']['source']
            potentialActionIdentifier = "" if (len(action["object"]) < 2) else action['object'][node_nr_action]['value']
            a = urlparse(source)
            filename = os.path.basename(a.path)
            name_only = os.path.splitext(filename)[0]
            digitaldocument_id = action['object'][node_nr]['nodeValue']['identifier']
            controlaction_id = action['identifier']
            # TODO: check if filename is pdf or other allowed format
            if controlaction_id in blacklist:
                continue
            if controlaction_id in known_campaigns:
                continue
            if controlaction_id not in known_campaigns:
                print(controlaction_id, " not currently known")
                identifier = store_sheet(source, potentialActionIdentifier, digitaldocument_id, controlaction_id)
                known_campaigns.append(controlaction_id)
                message = {'score_name': name_only, '_id': identifier}
                add_to_queue(cfg.mq_omr_planner, cfg.mq_omr_planner, json.dumps(message))
                # send message to omr planner that there is a new sheet

def update_control_action_status(identifier, action_status):
    url = CE_SERVER
    # ActiveActionStatus,
    # CompletedActionStatus,
    # FailedActionStatus,
    # PotentialActionStatus

    myclient = pymongo.MongoClient(MONGO_SERVER.ip, MONGO_SERVER.port)
    mydb = myclient[MONGO_DB]
    mycol = mydb[cfg.col_submitted_task]

    entry = mycol.find_one({"task_id": identifier, "digitaldocument_id": {"$exists": True} })
    ce_identifier = entry['ce_identifier']

    payload = "{\"query\":\"mutation{\\n  UpdateControlAction(\\n    identifier: \\\"%s\\\",\\n    actionStatus: %s\\n  )\\n  {\\n    identifier,\\n    actionStatus,\\n    url\\n  }\\n}\"}"
    payload = payload % (ce_identifier, action_status)
    headers = {'content-type': 'application/json'}
    response = requests.request("POST", url, data=payload, headers=headers)
    print(response.text)

def create_task_group(score_name, name, title):
    myclient = pymongo.MongoClient(MONGO_SERVER.ip, MONGO_SERVER.port)
    mydb = myclient[MONGO_DB]
    mycol = mydb[cfg.col_sheet]
    query = {'name': score_name}
    result = mycol.find_one(query)
    digitaldocument_id = result['digitaldocument_id']

    url = CE_SERVER
    entrypoint_id = ENTRYPOINT_ID
    potentialaction_id = "415fb8b5-c6ea-4c1d-be94-dd5fa8db4fd9"

    # get entrypointid and potentialactionid from settings.yaml
    payload = "{\"query\":\"mutation {\\n  taskGroup: RequestControlAction(\\n    controlAction: {\\n      entryPointIdentifier: \\\"%s\\\"\\n      potentialActionIdentifier: \\\"%s\\\"\\n      potentialAction: {\\n        name: \\\"%s\\\"\\n        title: \\\"%s\\\" }\\n    } )\\n  {\\n    identifier\\n  }\\n}\"}"
    headers = {'Content-Type': 'application/json'}
    payload = payload % (entrypoint_id, potentialaction_id, name, title)
    response = requests.request("POST", url, data=payload, headers=headers)
    data = json.loads(response.text)
    ce_id = data['data']['taskGroup']['identifier']

    # store the task group id, tasktype
    mycol = mydb["ce_taskgroups"]
    result = {
        "score_name": score_name,
        "controlaction_id": ce_id,
        "digitaldocument_id": digitaldocument_id,
        "name": name,
        "title": title
    }
    entry = mycol.insert_one(result).inserted_id
    # print(response.text)

    payload = "{\"query\":\"mutation {\\n  AddControlActionObject(\\n    from: { identifier: \\\"%s\\\" }\\n    to: { identifier: \\\"%s\\\" } ){\\n    from {\\n      identifier\\n    }\\n    to {\\n      identifier\\n    }\\n  }\\n}\"}"
    payload = payload % (ce_id, digitaldocument_id)
    headers = {'Content-Type': 'application/json'}
    response = requests.request("POST", url, data=payload, headers=headers)
    # data = json.loads(response.text)
    # print(response.text)
    # return ce_id

def create_task(task_id, task_type):
    myclient = pymongo.MongoClient(MONGO_SERVER.ip, MONGO_SERVER.port)
    mydb = myclient[MONGO_DB]
    
    mycol = mydb[cfg.col_task]
    query = {'_id': ObjectId(task_id)}
    result = mycol.find_one(query)
    score_name = result['score']
    name = result['name']
    task_url = result['url']

    mycol = mydb["ce_taskgroups"]
    query = {'score_name': score_name, 'name': task_type}
    result = mycol.find_one(query)
    digitaldocument_id = result['digitaldocument_id']
    task_group_controlaction_id = result['controlaction_id']

    url = CE_SERVER
    entrypoint_id = ENTRYPOINT_ID
    potentialaction_id = "641b570b-612a-4fe2-9e81-d4e79f3a3ea6"

    payload = "{\"query\":\"mutation {\\n  RequestControlAction(\\n    controlAction: {\\n      entryPointIdentifier: \\\"%s\\\" \\n      potentialActionIdentifier: \\\"%s\\\" \\n      potentialAction: {\\n        name: \\\"%s\\\"\\n        url: \\\"%s\\\"\\n      }\\n    } ){\\n    identifier\\n  }\\n}\"}"
    headers = {'Content-Type': 'application/json'}
    payload = payload % (entrypoint_id, potentialaction_id, name, task_url)
    response = requests.request("POST", url, data=payload, headers=headers)
    data = json.loads(response.text)
    ce_task_id = data['data']['RequestControlAction']['identifier']

    # store the task group id, tasktype
    mycol = mydb["submitted_tasks"]
    result = {
        "ce_identifier": ce_task_id,
        "task_id": task_id,
        "digitaldocument_id": digitaldocument_id,
        "name": name,
        "title": name
    }
    entry = mycol.insert_one(result).inserted_id
    # print(response.text)

    payload = "{\"query\":\"mutation {\\n  AddControlActionObject(\\n    from: { \\n      identifier: \\\"%s\\\" \\n    }\\n    to: { \\n      identifier: \\\"%s\\\" \\n    } ){\\n    from {\\n      identifier\\n    }\\n    to \\n    {\\n      identifier\\n    }\\n  }\\n}\"}"
    headers = {'Content-Type': 'application/json'}
    payload = payload % (ce_task_id, digitaldocument_id)
    response = requests.request("POST", url, data=payload, headers=headers)

    payload = "{\"query\":\"mutation {\\n  AddControlActionWasGeneratedBy(\\n    from: { \\n      identifier: \\\"%s\\\" \\n    }\\n    to: { \\n      identifier: \\\"%s\\\" \\n    } \\n  ){\\n    from {\\n      identifier\\n    }\\n    to {\\n      identifier\\n    }\\n  } \\n}\"}"
    headers = {'Content-Type': 'application/json'}
    payload = payload % (ce_task_id, task_group_controlaction_id)
    response = requests.request("POST", url, data=payload, headers=headers)

def update_task_group(score_name, task_type, mei_url, action_status):
    myclient = pymongo.MongoClient(MONGO_SERVER.ip, MONGO_SERVER.port)
    mydb = myclient[MONGO_DB]

    mycol = mydb["ce_taskgroups"]
    query = {'score_name': score_name, 'name': task_type}
    result = mycol.find_one(query)
    task_group_controlaction_id = result['controlaction_id']

    url = CE_SERVER
    payload = "{\"query\":\"mutation {\\n  UpdateControlAction(\\n    identifier: \\\"%s\\\" \\n    url: \\\"%s\\\"\\n    actionStatus: %s \\n  ){\\n    identifier\\n    name\\n    url\\n    actionStatus\\n  } \\n}\"}"
    headers = {'Content-Type': 'application/json'}
    payload = payload % (task_group_controlaction_id, mei_url, action_status)
    response = requests.request("POST", url, data=payload, headers=headers)

def check_boilerplate():
    # query the CE to see if the boilerplate code has been executed
    boilderplate = True
    url = CE_SERVER
    # check if the TUD entryppoint is there
    entrypoint_id = ENTRYPOINT_ID
    payload = "{\"query\":\"{\\n  EntryPoint(identifier:\\\"%s\\\"){\\n    identifier,\\n    description,\\n    contributor,\\n    creator,\\n    format,\\n    language,\\n    source,\\n    name,\\n    actionApplication{\\n      identifier,\\n    },\\n    potentialAction{\\n      identifier\\n    },\\n    \\n  }\\n}\"}"
    headers = {'Content-Type': 'application/json'}
    payload = payload % (entrypoint_id)
    response = requests.request("POST", url, data=payload, headers=headers)
    data = json.loads(response.text)
    if response.status_code == 200 and (len(data['data']['EntryPoint']) > 0):
        # print(response.text, len(data['data']['EntryPoint']))
        print("Boilderplate OK: Entrypoint is in CE")
    else:
        print("Boilderplate Error: Entrypoint is not in CE")
        boilderplate = False

    # check if task group potentialAction is there
    payload = "{\"query\":\"query {\\n  ControlAction(identifier:\\\"%s\\\") {\\n    identifier\\n    name\\n    description\\n  }\\n}\\n\"}"
    potentialaction_id = "415fb8b5-c6ea-4c1d-be94-dd5fa8db4fd9"
    payload = payload % (potentialaction_id)
    headers = {'Content-Type': 'application/json'}
    response = requests.request("POST", url, data=payload, headers=headers)
    data = json.loads(response.text)
    if response.status_code == 200 and (len(data['data']['ControlAction']) > 0):
        # print(response.text, len(data['data']['EntryPoint']))
        print("Boilderplate OK: ControlAction for TaskGroups is in CE")
    else:
        print("Boilderplate Error: ControlAction for TaskGroups is not in CE")
        boilderplate = False

    # check of the task potantialAction is there
    payload = "{\"query\":\"query {\\n  ControlAction(identifier:\\\"%s\\\") {\\n    identifier\\n    name\\n    description\\n  }\\n}\\n\"}"
    potentialaction_id = "641b570b-612a-4fe2-9e81-d4e79f3a3ea6"
    payload = payload % (potentialaction_id)
    headers = {'Content-Type': 'application/json'}
    response = requests.request("POST", url, data=payload, headers=headers)
    data = json.loads(response.text)
    if response.status_code == 200 and (len(data['data']['ControlAction']) > 0):
        # print(response.text, len(data['data']['EntryPoint']))
        print("Boilderplate OK: ControlAction for Tasks is in CE")
    else:
        print("Boilderplate Error: ControlAction for Tasks is not in CE")
        boilderplate = False

    # it passed all the checks so we should be good to go
    return boilderplate

def main():
    if check_boilerplate():
        try:
            begin_time = datetime.now()
            sleep_in_minutes = 1
            while True:
                if((datetime.now() - begin_time).total_seconds() > (sleep_in_minutes * 60)):
                    print(datetime.now(), "Monitor Upload")
                    poll_controlactions()
                    begin_time = datetime.now()
                    print(datetime.now(), "Nap Time!")
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=RABBITMQ_ADDRESS,
                        port=RABBITMQ_PORT))
                channel = connection.channel()
                channel.queue_declare(queue=cfg.mq_ce_communicator)
                method_frame, header_frame, body = channel.basic_get(cfg.mq_ce_communicator)
                if method_frame:
                    print(datetime.now(), str(body, 'utf-8'))
                    msg = json.loads(body.decode("utf-8"))
                    # forward message to ce
                    if msg['action'] == 'task type created':
                        create_task_group(
                            msg['score_name'],     # score name, the name field in the sheets collection
                            msg['name'],           # name of the task type
                            msg['title'])          # title of the task type, could be the same as the name for now
                    elif msg['action'] == 'task created':
                        create_task(
                            msg['task_id'],        # _id of the task in the tasks collection
                            msg['task_type'])      # name of the task type, same name as the one used when creating the task type above
                    elif msg['action'] == 'task group completed':
                        update_task_group(
                            msg['score_name'],     # score name, the name field in the sheets collection
                            msg['task_type'],      # name of the task type
                            msg['mei_url'],        # mei url that points to the correct branch and commit for this task group
                            'CompletedActionStatus')
                    elif msg['action'] == 'task completed':
                        update_control_action_status(
                            msg['_id'],            # _id of the task in the tasks collection
                            'CompletedActionStatus')
                    channel.basic_ack(method_frame.delivery_tag)
                channel.close()
                connection.close()
        except KeyboardInterrupt:
            print('interrupted!')


if __name__ == "__main__":
    main()
