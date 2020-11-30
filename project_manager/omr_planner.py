import pika
import yaml
from datetime import datetime
import json
import sys
sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
from collections import namedtuple

from pymongo import MongoClient
from bson.objectid import ObjectId

rabbitmq_address = cfg.rabbitmq_address
client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
db = client[cfg.db_name]
scheduled_messages = {}


def schedule_message(trigger, message, queue):
    print(f"Scheduling message {message} on trigger {trigger} for queue {queue}")
    scheduled_messages[trigger] = (message, queue)

def check_scheduled_messages(trigger):
    if trigger in scheduled_messages:
        message, queue = scheduled_messages.pop(trigger)
        send_message(
            queue,
            queue,
            json.dumps(message)
        )

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
                module = score_status['module']
                score_name = score_status['name']
                check_scheduled_messages((module, score_name))
                if score_status['module'] == 'measure_detector':
                    # Probably a good time to create a campaign-status object,
                    # only insert campaign status if it doesn't exist already though
                    campaign_status = {
                        "score_name": score_name, 
                        "stages_done": [], 
                        "task_types_done": [],
                        "finished": False
                    } 
                    db[cfg.col_campaign_status].update_one({"score_name": score_name}, {"$setOnInsert": campaign_status}, upsert=True)


                    mycol = db[cfg.col_sheet]
                    myquery = {"name" : score_status['name']}
                    mydoc = mycol.find_one(myquery)
                    if('submitted_mei_path' in mydoc and len(mydoc['submitted_mei_path']) > 0):
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
                    # First check campaign status
                    campaign_status = db[cfg.col_campaign_status].find_one({"score_name": score_name})
                    if campaign_status["finished"]:
                        print(f"Campaign for {score_name} has finished, no need to do more tasks")
                    else:
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
                                'action': 'start_next_stage'}))

                    continue
                if score_status['module'] == 'task_scheduler' and 'stage' in score_status:
                    # Task scheduler will always mention stage
                    stage = score_status['stage']
                    print(f"Completed stage {stage}")

                    # For now just send to slicer
                    # TODO: maybe later we should send to aligner first?
                    if stage==0:
                        print("Sending to slicer to rebuild")
                        send_message(
                        cfg.mq_slicer,
                        cfg.mq_slicer,
                        json.dumps({
                            '_id': score_status['_id'],
                            'name': score_status['name']}))
                    continue
                if score_status['module'] == 'task_scheduler' and 'task_type' in score_status:
                    # Here we should let the CE know some task type has been completed
                    task_type = score_status['task_type']
                    print(f"Completed {task_type} for score {score_name}")
                    continue


                if score_status['module'] == 'github_init':
                    # communicate to ce that github repo has been initiated
                    continue
                if score_status['module'] == 'github_update':
                    # communicate with ce that github repo has been updated
                    continue

    except KeyboardInterrupt:
        print('INTERRUPTED!')


if __name__ == "__main__":
    main()
