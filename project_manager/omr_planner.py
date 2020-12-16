import pika
import yaml
from datetime import datetime
import json
import sys
import functools
sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm

from pymongo import MongoClient
from bson.objectid import ObjectId


def check_dependencies(score_name, modules):
    return all([(score_name, m) in ready_dependencies for m in modules])

def update_dependency(score_name, module):
    ready_dependencies.add((score_name, module))

def send_message(message, queue, channel):
    json_str = json.dumps(message)
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange='', routing_key=queue, body=json_str)

# TODO: ack implementation is a bit rudimentary for now, in the long term we would like
#       to only ack when the work was completed succesfully. For this we need to store
#       delivery tags here and pass them back-and-forth to the modules so we can ack
#       in the callback
def ack_message(channel, delivery_tag):
    if channel.is_open:
        channel.basic_ack(delivery_tag)
    else:
        print("Warning, could not acknowledge message, channel was closed!")


# Status callback handler
# message spec:
#   name        :   name of score
#   module      :   module message originated from
#
# --- optional fields depending on source ---
#   stage       :   used by task_scheduler to indicate completed stage
#   task_type   :   used by task_scheduler to indicate completed task type
def take_action_on_status(channel, method, properties, body):
    score_status = json.loads(body)
    module = score_status['module']
    score_name = score_status['name']
    update_dependency(score_name, module)
    delivery_tag = method.delivery_tag
    ack = functools.partial(ack_message, channel, delivery_tag)
    # check_scheduled_messages((module, score_name), channel)
    if module == 'measure_detector':
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
        myquery = {"name": score_status['name']}
        mydoc = mycol.find_one(myquery)
        if('submitted_mei_path' in mydoc and len(mydoc['submitted_mei_path']) > 0):
            print(
                datetime.now(),
                'sending ',
                score_status['name'], 'to aligner')
            send_message(
                {
                    '_id': score_status['_id'],
                    'partials': [mydoc['submitted_mei_path']],
                    'name': score_status['name']
                },
                cfg.mq_aligner,
                channel
                )
        else:
            print(
                datetime.now(),
                'sending ',
                score_status['name'], 'to slicer')
            send_message(
                {
                    '_id': score_status['_id'],
                    'name': score_status['name']
                },
                cfg.mq_slicer,
                channel
            )
            print(
                datetime.now(),
                'sending ',
                score_status['name'], 'to github_init')
            send_message(
                {
                    '_id': score_status['_id'],
                    'name': score_status['name']
                },
                cfg.mq_github_init,
                channel
            )
        ack()
    elif module == 'aligner':
        print(
            datetime.now(),
            'sending ',
            score_status['name'], 'to slicer')
        send_message(
            {
                '_id': score_status['_id'],
                'name': score_status['name']
            },
            cfg.mq_slicer,
            channel
        )
        print(
            datetime.now(),
            'sending ',
            score_status['name'], 'to github_init')
        send_message(
            {
                '_id': score_status['_id'],
                'name': score_status['name']
            },
            cfg.mq_github_init,
            channel
        )
        ack()
    elif module == 'slicer':
        # First check campaign status
        campaign_status = db[cfg.col_campaign_status].find_one({"score_name": score_name})
        if campaign_status["finished"]:
            print(f"Campaign for {score_name} has finished, no need to do more tasks")
        else:
            if check_dependencies(score_name, {'slicer', 'github_init'}) or not cfg.github_enable:
                print(
                    datetime.now(),
                    'sending ',
                    score_status['name'], 'task_scheduler')
                send_message(
                    {
                        '_id': score_status['_id'],
                        'name': score_status['name'],
                        'action': 'start_next_stage'
                    },
                    cfg.mq_task_scheduler,
                    channel
                )
        ack()
    elif module == 'task_scheduler' and 'stage' in score_status:
        # Task scheduler will always mention stage
        stage = score_status['stage']
        print(f"Completed stage {stage}")

        # For now just send to slicer
        # TODO: maybe later we should send to aligner first?
        if stage in {0, 1}:
            print("Sending to slicer to rebuild")
            send_message(
                {
                    '_id': score_status['_id'],
                    'name': score_status['name']
                },
                cfg.mq_slicer,
                channel
            )
        ack()
    elif module == 'task_scheduler' and 'task_type' in score_status:
        # Here we should let the CE know some task type has been completed
        task_type = score_status['task_type']
        url = f"https://github.com/{cfg.github_organization}/{score_name}/tree/{cfg.github_branch}"
        send_message(
            {
                "action": "task group completed",
                "score_name": score_name,
                "task_type": task_type,
                "mei_url": url
            },
            cfg.mq_ce_communicator,
            channel
        )
        print(f"Completed {task_type} for score {score_name}")
        ack()
    elif module == 'github_init':
        # Temporary solution until something better can be found
        # TODO: We need a way to deal with waiting for multiple messages, possibly using the same
        #       delivery-tag method that proper acks will use

        # Depending on which of the two finishes sooner, one of them will only send
        if check_dependencies(score_name, {'slicer', 'github_init'}) and cfg.github_enable:
            print(
                datetime.now(),
                'sending ',
                score_status['name'], 'task_scheduler')
            send_message(
                {
                    '_id': score_status['_id'],
                    'name': score_status['name'],
                    'action': 'start_next_stage'
                },
                cfg.mq_task_scheduler,
                channel
            )
        ack()


# Main callback handler, for incoming new scores
# message spec:
#   name    :   name of score
def take_action(channel, method, properties, body):
    score = json.loads(body)
    if len(score) > 0:
        print(
            datetime.now(),
            'new score: ',
            score['score_name'],
            'sending to measure_detector')
        if score['_id'] is not None:
            message = {'score_name': score['score_name'], '_id': score['_id']}
            send_message(message, cfg.mq_new_item, channel)
            ack_message(channel, method.delivery_tag)


if __name__ == "__main__":
    # Connect to db
    mongo_client = MongoClient(*cfg.mongodb_address)
    db = mongo_client[cfg.db_name]

    # Connect to mq and set up callbacks
    parameters = pika.ConnectionParameters(*cfg.rabbitmq_address)
    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()
    channel.queue_declare(queue=cfg.mq_omr_planner)
    channel.queue_declare(queue=cfg.mq_omr_planner_status)
    channel.basic_consume(
        on_message_callback=take_action,
        queue=cfg.mq_omr_planner
    )
    channel.basic_consume(
        on_message_callback=take_action_on_status,
        queue=cfg.mq_omr_planner_status
    )

    ready_dependencies = set()

    print('OMRP is listening...')
    channel.start_consuming()
