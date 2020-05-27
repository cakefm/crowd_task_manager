import pika
import yaml
from datetime import datetime
import json
import sys
sys.path.append("..")
import common.settings as settings
import common.file_system_manager as fsm

from pymongo import MongoClient
from bson.objectid import ObjectId

with open("../settings.yaml", "r") as file:
    config = yaml.safe_load(file.read())

rabbitmq_address = config['rabbitmq_address']
address = rabbitmq_address.split(":")
client = MongoClient(settings.mongo_address[0], int(settings.mongo_address[1]))
db = client.trompa_test


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
        channel.basic_ack(method_frame.delivery_tag)
    channel.close()
    connection.close()
    return msg


def send_message(queue_name, routing_key, message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=address[0],
            port=address[1]))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=routing_key, body=message)
    connection.close()


def check_for_omr_project(queue_name):
    message = read_message(queue_name)
    return message


def call_module(module_name, score_name, score_id):
    queue_name = module_name + '_queue'
    routing_key = queue_name
    message = {'score_name': score_name, '_id': score_id}
    send_message(queue_name, routing_key, json.dumps(message))


def main():
    try:
        print(datetime.now(), 'omr_planner started!')
        while True:
            score = read_message('omr_planner_queue')
            if len(score) > 0:
                print(
                    datetime.now(),
                    'new score: ',
                    score['score_name'],
                    'sending to measure_detector')
                if score['_id'] is not None:
                    call_module(
                        'measure_detector',
                        score['score_name'],
                        score['_id'])
            score_status = read_message('omr_planner_status_queue')
            if len(score_status) > 0:
                if score_status['module'] == 'measure_detector':
                    mycol = db['sheets']
                    myquery = {"name" : score_status['name']}
                    mydoc = mycol.find_one(myquery)
                    if(mydoc['submitted_mei_path']):
                        if(len(mydoc['submitted_mei_path']) > 0):
                            print(
                                datetime.now(),
                                'sending ',
                                score_status['name'], 'to aligner')
                            send_message(
                                'aligner_queue',
                                'aligner_queue',
                                json.dumps({
                                    '_id': score_status['_id'],
                                    'partials': [mydoc['submitted_mei_path']],
                                    'name': score_status['name']}))
                            continue
                    else:
                        print(
                            datetime.now(),
                            'sending ',
                            score_status['name'], 'to slicer')
                        send_message(
                            'slicer_queue',
                            'slicer_queue',
                            json.dumps({
                                '_id': score_status['_id'],
                                'name': score_status['name']}))
                        print(
                            datetime.now(),
                            'sending ',
                            score_status['name'], 'to github_init')
                        send_message(
                            'github_init_queue',
                            'github_init_queue',
                            json.dumps({
                                '_id': score_status['_id'],
                                'name': score_status['name']}))
                        continue
                if score_status['module'] == 'aligner':
                    print(
                        datetime.now(),
                        'sending ',
                        score_status['name'], 'to slicer')
                    send_message(
                        'slicer_queue',
                        'slicer_queue',
                        json.dumps({
                            '_id': score_status['_id'],
                            'name': score_status['name']}))
                    print(
                        datetime.now(),
                        'sending ',
                        score_status['name'], 'to github_init')
                    send_message(
                        'github_init_queue',
                        'github_init_queue',
                        json.dumps({
                            '_id': score_status['_id'],
                            'name': score_status['name']}))
                    continue
                if score_status['module'] == 'slicer':
                    print(
                        datetime.now(),
                        'sending ',
                        score_status['name'], 'task_scheduler')
                    send_message(
                        'task_scheduler_queue',
                        'task_scheduler_queue',
                        json.dumps({
                            '_id': score_status['_id'],
                            'name': score_status['name'],
                            'action': 'create_edit_tasks'}))
                    continue
                if score_status['module'] == 'aggregator':
                    # send message to score_rebuilder_queue
                    print(
                        datetime.now(),
                        'sending ',
                        score_status['name'], 'to score_rebuilder')
                    send_message(
                        'score_rebuilder_queue',
                        'score_rebuilder_queue',
                        json.dumps({
                            'task_id': score_status['_id'],
                            'name': score_status['name']}))
                    # print(
                    #     datetime.now(),
                    #     'sending ',
                    #     score_status['name'], 'to task_scheduler')
                    # send_message(
                    #     'task_scheduler_queue',
                    #     'task_scheduler_queue',
                    #     json.dumps({
                    #         '_id': score_status['_id'],
                    #         'action': 'create_verify_task',
                    #         'name': score_status['name']}))
                    continue
                if score_status['module'] == 'github_init':
                    # communicate to ce that github repo has been initiated
                    continue
                if score_status['module'] == 'github_update':
                    # communicate with ce that github repo has been updated
                    continue
                if score_status['module'] == 'score_rebuilder':
                    # send message to github update?
                    print(
                        datetime.now(),
                        'sending ',
                        score_status['name'], 'to github_update')
                    send_message(
                        'github_queue',
                        'github_queue',
                        json.dumps({
                            'task_id': score_status['task_id'],
                            'name': score_status['name']}))
                    continue
    except KeyboardInterrupt:
        print('INTERRUPTED!')


if __name__ == "__main__":
    main()
