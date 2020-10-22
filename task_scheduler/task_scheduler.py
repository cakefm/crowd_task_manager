import pika
import yaml
import json
import sys
sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm

from pymongo import MongoClient
from bson.objectid import ObjectId
from shutil import copyfile
import pathlib
from pathlib import Path
import os
from datetime import datetime
import xml.etree.ElementTree as ET

rabbitmq_address = cfg.rabbitmq_address
client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
db = client[cfg.db_name]


def send_message(queue_name, routing_key, message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbitmq_address.ip,
            port=rabbitmq_address.port))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=routing_key, body=message)
    connection.close()


def read_message(queue_name):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbitmq_address.ip,
            port=rabbitmq_address.port))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    msg = ''
    method_frame, header_frame, body = channel.basic_get(queue_name)
    if method_frame:
        msg = json.loads(body.decode("utf-8"))
        # print(msg)
        channel.basic_ack(method_frame.delivery_tag)
    channel.close()
    connection.close()
    # print(msg)
    return msg


def create_task_from_slice(measure_slice, status):
    begin = measure_slice['start']
    end = measure_slice['end']
    
    # TODO: Slice type would be nice so we don't have to do this check
    # right now we assume lines are the biggest possible slices
    difference = end - begin
    types = {
        1 : "measures",
        2 : "double_measures"
    }
    slice_type = "lines" if (difference > 2) else types[difference]
    slice_location = str(fsm.get_sheet_slices_directory(measure_slice['score']) / slice_type / measure_slice['name'])
    copy_destination = str(fsm.get_sheet_api_directory(measure_slice['score'], slice_type=slice_type) / measure_slice['name'])

    task = {
        'name': measure_slice['name'],
        'score': measure_slice['score'],
        'slice_id': str(measure_slice['_id']),
        'image_path': slice_location,
        'status': status,
        'xml': getXMLofSlice(measure_slice['score'], measure_slice['start'], measure_slice['end'])
        }
    entry = db[cfg.col_task].insert_one(task).inserted_id

    copyfile(slice_location, copy_destination)
    return str(entry)


def getXMLofSlice(score, slice_begin_n, slice_end_n):
    mycol = db[cfg.col_score]
    myquery = {"name": score}
    mydoc = mycol.find_one(myquery)
    end_xml = ""
    # measure_tag_begin = "<measure>"
    # measure_tag_end = "</measure>"
    measure_tag_begin = ""
    measure_tag_end = ""
    for x in range(slice_begin_n, slice_end_n):
        if(len(mydoc['measures'][x]['xml']) > 0):
            end_xml = end_xml + \
                measure_tag_begin + \
                mydoc['measures'][x]['xml'] + \
                measure_tag_end
    return end_xml


def submit_task_to_ce(task_id):
    send_message(
        cfg.mq_ce_communicator,
        cfg.mq_ce_communicator,
        json.dumps({'action': 'edit task created', '_id': task_id}))
    return ''


def create_context_for_tasks(score_name):
    mycol = db[cfg.col_sheet]
    myquery = {"name": score_name}
    mydoc = mycol.find_one(myquery)
    mei_path = mydoc['mei_path'][0]
    pages = mydoc['pages_path']

    tree = ET.parse(mei_path)
    root = tree.getroot()

    mapping = {}
    thing = root.find('{http://www.music-encoding.org/ns/mei}music')
    thing2 = thing.find('{http://www.music-encoding.org/ns/mei}facsimile')
    for thing3 in thing2.findall('{http://www.music-encoding.org/ns/mei}surface'):
        for child in thing3.findall('{http://www.music-encoding.org/ns/mei}zone'):
            zone_id = child.get('{http://www.w3.org/XML/1998/namespace}id')
            coordinates = {}
            coordinates['ulx'] = child.get('ulx')
            coordinates['uly'] = child.get('uly')
            coordinates['lrx'] = child.get('lrx')
            coordinates['lry'] = child.get('lry')
            mapping['#' + zone_id] = coordinates

    mycol = db[cfg.col_task]
    myquery = {"score": score_name}
    mydoc = mycol.find(myquery)
    task_context_collection = []
    for task in mydoc:
        task_context = {}
        task_context['task_id'] = task['_id']
        task_context['preface'] = ''
        task_context['postface'] = ''
        task_context['image_path'] = task['image_path']
        task_context['score'] = task['score']

        mycol2 = db[cfg.col_slice]
        myquery2 = {"_id": ObjectId(task['slice_id'])}
        mydoc2 = mycol2.find_one(myquery2)
        start = mydoc2['start']
        
        mycol3 = db[cfg.col_score]
        myquery3 = {"name": mydoc2['score']}
        mydoc3 = mycol3.find_one(myquery3)
        xml = mydoc3['measures'][start]['xml']

        task_context['page_nr'] = mydoc3['measures'][start]['page_index']

        # xml = task['xml']
        tree = ET.fromstring(xml)
        zone_id = tree.get('facs')
        coords = mapping[zone_id]
        task_context['coords'] = coords
        task_context_collection.append(task_context)

    mycol4 = db[cfg.col_task_context]
    mycol4.insert_many(task_context_collection)

    # TODO: test this copy files
    i = 0
    for page in pages:
        api_folder = str(os.path.abspath(os.path.join(os.getcwd(), '..', 'api')))
        pathlib.Path(api_folder + "/static/" + score_name + "/pages/").mkdir(parents=True, exist_ok=True)
        copy_folder = page
        copy_dest = api_folder + "/static/" + score_name + "/pages/page_" + str(i) + ".jpg"
        i = i + 1
        copyfile(copy_folder, copy_dest)
    return ''


def main():
    try:
        print(datetime.now(), 'start task_scheduler')
        while True:
            # read task_scheduler_queue
            data = read_message(cfg.mq_task_scheduler)
            if data != '':
                if(data['action'] == 'create_edit_tasks'):
                    print('reading task scheduler queue')
                    score = data['name']

                    mycol = db[cfg.col_slice]
                    myquery = {"score": score}
                    mydoc = mycol.find(myquery)
                    for measure_slice in mydoc:
                        slice_length = measure_slice['end'] - measure_slice['start']
                        if(slice_length == 1):
                            mycol2 = db[cfg.col_sheet]
                            myquery2 = {"name": score}
                            mydoc2 = mycol2.find_one(myquery2)

                            # TODO: we need a way to specify the type of task without depending on the CE
                            # for testing purposes. For now, always choose 'annotation'
                            if mydoc2['source'] == 'CE':
                                status = 'verification' if (mydoc2['edit_action'] == mydoc2['verify_action']) else 'annotation'
                            else:
                                status = 'annotation'

                            task_id = create_task_from_slice(measure_slice, status)
                            print(datetime.now(), 'created task ', task_id)
                            if((mydoc2['source'] == 'CE') and (mydoc2['edit_action'] != mydoc2['verify_action']) ):
                                submit_task_to_ce(task_id)
                                print(
                                    datetime.now(),
                                    'sent message to ce_communicator for ',
                                    task_id)
                            elif((mydoc2['source'] == 'CE') and (mydoc2['edit_action'] == mydoc2['verify_action']) ):
                                send_message(
                                    cfg.mq_ce_communicator,
                                    cfg.mq_ce_communicator,
                                    json.dumps({
                                        'action': 'verify task created',
                                        '_id': task_id}))
                    create_context_for_tasks(score)
            status_data = read_message(cfg.mq_task_scheduler_status)
            if status_data != '':
                # get status update from api,
                # new result received
                # check if task is verify or edit,
                # if enough, then send msg to aggregator
                if status_data['module'] == 'api':
                    tasks_col = db[cfg.col_task]
                    tasks_query = {"_id": ObjectId(status_data['identifier'])}
                    tasks_doc = tasks_col.find_one(tasks_query)

                    if (tasks_doc['status'] == 'annotation') and (status_data['type'] == 'edit'):
                        mycol = db[cfg.col_result]
                        myquery = {
                            "task_id": status_data['identifier'],
                            "result_type": "edit"}
                        mydoc = mycol.find(myquery)
                        if (mydoc.count() == 1):
                            mycol2 = db[cfg.col_sheet]
                            myquery2 = {"name": tasks_doc['score']}
                            mydoc2 = mycol2.find_one(myquery2)
                            if(mydoc2['source'] == 'CE'):
                                send_message(
                                    cfg.mq_ce_communicator,
                                    cfg.mq_ce_communicator,
                                    json.dumps({
                                        'action': 'task active',
                                        'identifier': status_data['identifier'],
                                        'type': 'edit',
                                        'status': 'ActiveActionStatus'}))
                        elif mydoc.count() > 2:
                            send_message(
                                cfg.mq_aggregator_xml,
                                cfg.mq_aggregator_xml,
                                json.dumps({'task_id': status_data['identifier']})
                                )
                    elif (tasks_doc['status'] == 'verification') and (status_data['type'] == 'verify'):
                        mycol = db[cfg.col_result]
                        myquery = {
                            "task_id": status_data['identifier'],
                            "result_type": "verify"}
                        mydoc = mycol.find(myquery)
                        results_count = mydoc.count()

                        mycol = db[cfg.col_result]
                        myquery = {
                            "task_id": status_data['identifier'],
                            "result_type": "verify",
                            "opinion": True}
                        mydoc = mycol.find(myquery)
                        results_count_true = mydoc.count()

                        ratio = results_count_true / results_count

                        if (results_count >= 3) and (ratio >= 0.6):
                            mycol = db[cfg.col_task]
                            myquery = {"_id": ObjectId(status_data['identifier'])}

                            update_thing = {'$set': {'status': "complete"}}
                            mydoc = mycol.update_one(myquery, update_thing)

                            mycol2 = db[cfg.col_sheet]
                            myquery2 = {"name": tasks_doc['score']}
                            mydoc2 = mycol2.find_one(myquery2)
                            if(mydoc2['source'] == 'CE'):
                                send_message(
                                    cfg.mq_ce_communicator,
                                    cfg.mq_ce_communicator,
                                    json.dumps({
                                        'action': 'task completed',
                                        'identifier': status_data['identifier'],
                                        'type': 'verify',
                                        'status': 'CompletedActionStatus'}))

                            mycol = db[cfg.col_task]
                            myquery = {"_id": ObjectId(status_data['identifier'])}
                            mydoc = mycol.find_one(myquery)

                            send_message(
                                cfg.mq_omr_planner_status,
                                cfg.mq_omr_planner_status,
                                json.dumps({
                                    'module': 'task_scheduler',
                                    '_id': status_data['identifier'],
                                    'name': mydoc['score']}))
                        elif (results_count >= 3) and (ratio < 0.6):
                            mycol = db[cfg.col_task]
                            myquery = {"_id": ObjectId(status_data['identifier'])}

                            update_thing = {'$set': {'status': "annotation"}}
                            mydoc = mycol.update_one(myquery, update_thing)

                            # delete documents with verification results
                            mycol = db[cfg.col_result]
                            myquery = {
                                "task_id": status_data['identifier'],
                                "result_type": "verify"}
                            mydoc = mycol.delete_many(myquery)

                            mycol = db[cfg.col_result]
                            myquery = {
                                "task_id": status_data['identifier'],
                                "result_type": "edit"}
                            mydoc = mycol.delete_many(myquery)

                            mycol2 = db[cfg.col_sheet]
                            myquery2 = {"name": tasks_doc['score']}
                            mydoc2 = mycol2.find_one(myquery2)
                            if(mydoc2['source'] == 'CE'):
                                send_message(
                                    cfg.mq_ce_communicator,
                                    cfg.mq_ce_communicator,
                                    json.dumps({
                                        'action': 'task completed',
                                        'identifier': status_data['identifier'],
                                        'type': 'verify',
                                        'status': 'FailedActionStatus'}))
                                if(mydoc2['edit_action'] != mydoc2['verify_action']):
                                    send_message(
                                        cfg.mq_ce_communicator,
                                        cfg.mq_ce_communicator,
                                        json.dumps({
                                            'action': 'task completed',
                                            'identifier': status_data['identifier'],
                                            'type': 'edit',
                                            'status': 'PotentialActionStatus'}))
                if status_data['module'] == 'aggregator_xml':
                    if status_data['status'] == 'complete':
                        mycol = db[cfg.col_aggregated_result]
                        myquery = {"task_id": status_data['_id']}
                        new_xml = mycol.find_one(myquery)['xml']

                        mycol = db[cfg.col_task]
                        myquery = {"_id": ObjectId(status_data['_id'])}
                        update_thing = {'$set': {'status': "verification", 'xml': new_xml}}
                        mydoc = mycol.update_one(myquery, update_thing)
                        tasks_doc = mycol.find_one(myquery)

                        mycol2 = db[cfg.col_sheet]
                        myquery2 = {"name": tasks_doc['score']}
                        mydoc2 = mycol2.find_one(myquery2)
                        if(mydoc2['source'] == 'CE'):
                            send_message(
                                cfg.mq_ce_communicator,
                                cfg.mq_ce_communicator,
                                json.dumps({
                                    'action': 'task completed',
                                    'identifier': status_data['_id'],
                                    'type': 'edit',
                                    'status': 'CompletedActionStatus'}))

                            send_message(
                                cfg.mq_ce_communicator,
                                cfg.mq_ce_communicator,
                                json.dumps({
                                    'action': 'verify task created',
                                    '_id': status_data['_id']}))
                    if status_data['status'] == 'failed':
                        # delete documents with verification results
                        mycol = db[cfg.col_result]
                        myquery = {
                            "task_id": status_data['_id'],
                            "result_type": "edit"}
                        mydoc = mycol.delete_many(myquery)

                # if verify task, then count how many and then mark complete
                # if majority say that it is wrong, then set edit task as incomplete

                # get status update from aggregator
                # mark task as complete
                # send msg to ce_comm
                # create verify task ce
                # update xml of task?


                # elif(data['action'] == 'create_edit_tasks'):
                #     print('creating verify task')

                #     mycol = db['tasks']
                #     myquery = {"_id": ObjectId(data['_id'])}
                #     mydoc = mycol.find_one(myquery)

                #     mycol2 = db['results']
                #     myquery2 = {"task_id": data['_id']}
                #     mydoc2 = mycol2.find(myquery2)
                #     end_result = mydoc2[0]['xml']

                #     for result in mydoc2:
                #         end_result = result['xml']

                #     new_task = {
                #         'name': mydoc['name'],
                #         'score': mydoc['score'],
                #         'slice_id': mydoc['slice_id'],
                #         'image_path': mydoc['image_path'],
                #         'xml': end_result
                #     }

                #     entry = db["tasks"].insert_one(new_task).inserted_id


    except KeyboardInterrupt:
        print('interrupted')


if __name__ == "__main__":
    main()
