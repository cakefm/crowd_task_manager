import pika
import yaml
import json
import sys
sys.path.append("..")
import common.settings as settings
import common.file_system_manager as fsm

from pymongo import MongoClient
from bson.objectid import ObjectId
from shutil import copyfile
import pathlib
from pathlib import Path
import os
from datetime import datetime

with open("../settings.yaml", "r") as file:
    config = yaml.safe_load(file.read())

rabbitmq_address = config['rabbitmq_address']
address = rabbitmq_address.split(":")
client = MongoClient(settings.mongo_address[0], int(settings.mongo_address[1]))
db = client.trompa_test


def send_message(queue_name, routing_key, message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=address[0],
            port=address[1]))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=routing_key, body=message)
    connection.close()


def read_message(queue_name):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=address[0],
            port=address[1]))
    channel = connection.channel()
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


def create_task_from_slice(measure_slice):
    begin = measure_slice['start']
    end = measure_slice['end']
    difference = end - begin
    subfolder = ["", "/slices/measures/", "/slices/double_measures/"]
    subfolder = "/slices/lines/" if (end - begin) > 2 else subfolder[difference]
    api_folder = str(os.path.abspath(os.path.join(os.getcwd(), '..', 'api')))
    task = {
        'name': measure_slice['name'],
        'score': measure_slice['score'],
        'slice_id': str(measure_slice['_id']),
        'image_path': measure_slice['score'] + subfolder + measure_slice['name'],
        'xml': getXMLofSlice(measure_slice['score'], measure_slice['start'], measure_slice['end'])}
    entry = db["tasks"].insert_one(task).inserted_id

    pathlib.Path(api_folder + "/static/" + task['score'] + "/slices/measures/").mkdir(parents=True, exist_ok=True)
    copy_folder = str(Path.home()) + "/omr_files/" + task['score'] + subfolder + task['name']
    copy_dest = api_folder + "/static/" + task['score'] + "/slices/measures/" + task['name']
    copyfile(copy_folder, copy_dest)
    return str(entry)


def getXMLofSlice(score, slice_begin_n, slice_end_n):
    mycol = db["scores"]
    myquery = {"name": score}
    mydoc = mycol.find_one(myquery)
    end_xml = ""
    # measure_tag_begin = "<measure>"
    # measure_tag_end = "</measure>"
    measure_tag_begin = ""
    measure_tag_end = "</measure>"
    for x in range(slice_begin_n, slice_end_n):
        if(len(mydoc['measures'][x]['xml']) > 0):
            end_xml = end_xml + \
                measure_tag_begin + \
                mydoc['measures'][x]['xml'][:-1] + \
                measure_tag_end
    return end_xml


def submit_task_to_ce(task_id):
    send_message(
        'ce_communicator_queue',
        'ce_communicator_queue',
        json.dumps({'action': 'task created', '_id': task_id}))
    return ''


def main():
    try:
        print(datetime.now(), 'start task_scheduler')
        while True:
            # read task_scheduler_queue
            data = read_message('task_scheduler_queue')
            if data != '':
                print('reading task_scheduler_queue')
                score = data['name']

                mycol = db['slices']
                myquery = {"score": score}
                mydoc = mycol.find(myquery)
                for measure_slice in mydoc:
                    task_id = create_task_from_slice(measure_slice)
                    print(datetime.now(), 'created task ', task_id)
                    submit_task_to_ce(task_id)
                    print(
                        datetime.now(),
                        'sent message to ce_communicator for ',
                        task_id)
    except KeyboardInterrupt:
        print('interrupted')


if __name__ == "__main__":
    main()
