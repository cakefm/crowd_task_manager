import pika
import yaml
from datetime import datetime
import json
import sys
sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm

from pymongo import MongoClient
from bson.objectid import ObjectId

rabbitmq_address = cfg.rabbitmq_address
client = MongoClient(cfg.mongo_address.ip, cfg.mongo_address.port)
db = client[cfg.db_name]


def read_message(queue_name):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbitmq_address.ip,
            port=rabbitmq_address.port))
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
            host=rabbitmq_address.ip,
            port=rabbitmq_address.port))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=routing_key, body=message)
    connection.close()


def check_for_omr_project(queue_name):
    message = read_message(queue_name)
    return message


def main():
    try:
        print(datetime.now(), 'omr_planner started!')
        while True:
            score = read_message(cfg.mq_omr_planner)
            if len(score) > 0:
                print(
                    datetime.now(),
                    'new score: ',
                    score['score_name'],
                    'sending to measure_detector')
                if score['_id'] is not None:
                    message = {'score_name': score['score_name'], '_id': score['_id']}
                    send_message(cfg.mq_new_item, cfg.mq_new_item, json.dumps(message))

            score_status = read_message(cfg.mq_omr_planner_status)
            if len(score_status) > 0:
                if score_status['module'] == 'measure_detector':
                    mycol = db[cfg.col_sheet]
                    myquery = {"name" : score_status['name']}
                    mydoc = mycol.find_one(myquery)
                    if('submitted_mei_path' in mydoc):
                        if(len(mydoc['submitted_mei_path']) > 0):
                            print(
                                datetime.now(),
                                'sending ',
                                score_status['name'], 'to aligner')
                            send_message(
                                cfg.mq_aligner,
                                cfg.mq_aligner,
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
                            cfg.mq_slicer,
                            cfg.mq_slicer,
                            json.dumps({
                                '_id': score_status['_id'],
                                'name': score_status['name']}))
                        print(
                            datetime.now(),
                            'sending ',
                            score_status['name'], 'to github_init')
                        send_message(
                            cfg.mq_github_init,
                            cfg.mq_github_init,
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
                        cfg.mq_slicer,
                        cfg.mq_slicer,
                        json.dumps({
                            '_id': score_status['_id'],
                            'name': score_status['name']}))
                    print(
                        datetime.now(),
                        'sending ',
                        score_status['name'], 'to github_init')
                    send_message(
                        cfg.mq_github_init,
                        cfg.mq_github_init,
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
                        cfg.mq_task_scheduler,
                        cfg.mq_task_scheduler,
                        json.dumps({
                            '_id': score_status['_id'],
                            'name': score_status['name'],
                            'action': 'create_edit_tasks'}))
                    continue
                if score_status['module'] == 'task_scheduler':
                    # send message to score_rebuilder_queue
                    print(
                        datetime.now(),
                        'sending ',
                        score_status['name'], 'to score_rebuilder')
                    send_message(
                        cfg.mq_score_rebuilder,
                        cfg.mq_score_rebuilder,
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
                        cfg.mq_github,
                        cfg.mq_github,
                        json.dumps({
                            'task_id': score_status['task_id'],
                            'name': score_status['name']}))
                    continue
    except KeyboardInterrupt:
        print('INTERRUPTED!')


if __name__ == "__main__":
    main()
