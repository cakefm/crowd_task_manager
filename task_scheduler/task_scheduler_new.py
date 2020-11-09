import pika
import yaml
import json
import sys
sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
from pymongo import MongoClient
from bson.objectid import ObjectId
from task_type import init_task_types
from shutil import copyfile
from bson.objectid import ObjectId

def getXMLofSlice(score_name, begin, end):
    score = db[cfg.col_score].find_one({"name": score_name})
    merged = "\n".join([m["xml"] for m in score["measures"][begin:end]])
    return f"<placeholderroot>{merged}</placeholderroot>"

def send_message(message, queue, channel):
    json_str = json.dumps(message)
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange="", routing_key=queue, body=json_str)

# Main callback handler
# spec:
#   name    :   name of score
#   action  :   action to take
def take_action(channel, method, properties, body):
    message = json.loads(body)
    action = message['action']

    actions = {
        "create_edit_tasks"         : [create_tasks_and_batches, submit_next_batch]
    }[action]

    for action in actions:
        action(message, channel)

def submit_next_batch(message, channel):
    unsubmitted_batches = [x for x in db[cfg.col_task_batch].find() if x["submitted"]==False]
    batches_per_task_type = {}
    for batch in unsubmitted_batches:
        if batch["task_type"] not in batches_per_task_type:
            batches_per_task_type[batch["task_type"]] = []
        batches_per_task_type[batch["task_type"]].append(batch)

    for task_type in batches_per_task_type:
        batches = batches_per_task_type[task_type]
        next_to_send = min(batches, key=lambda x: x["priority"])
        print(f"Sending task batch under key {next_to_send['priority']} of task type {task_type} containing: {next_to_send['tasks']}")
        for task_id in next_to_send["tasks"]:
            send_message(
                {'task_id': task_id},
                cfg.mq_ce_communicator,
                channel
            )
        db[cfg.col_task_batch].update_one({"_id": next_to_send["_id"]}, {"$set": {"submitted": True}})


# Status callback handler
# spec:
#   _id     :   the task id 
#   module  :   module this message was sent from 
#   status  :   optional, either "complete" or "failed"
def take_action_on_status(channel, method, properties, body):
    message = json.loads(body)
    module = message['module']
    # TODO: task status queue spec needs to be cleared up, so we can factor out actions
    status = message.get('status', None)
    log_status = lambda m, c: print(f"from {m['module']}: {m}")
    
    actions = {
        ("api", None)                           : [log_status, check_results],
        ("aggregator_xml", "complete")          : [log_status, submit_next_batch],
        ("aggregator_xml", "failed")            : [log_status],
        ("aggregator_form", "complete")         : [log_status, submit_next_batch],
        ("aggregator_form", "failed")           : [log_status]
    }[(module, status)]

    for action in actions:
        action(message, channel)


    # # TODO: should somehow factor out actions, does not make much sense for a status update
    # if 'action' in message:
    #     action = message['action']

    #     actions = {
    #         "result"            : [, check_results]
    #     }[action]



def check_results(message, channel):
    # what we do here:
    # - get the identifier of the task ("identifier")
    # - find all results so far for that task
    # - if it equals or exceeds minimum required responses, send a message to jeboi aggregator (either form or xml)
    # - when the aggregator manages to find a good enough aggregation of results, a different message should advance the step for tasks
    task_id = message["identifier"]
    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})
    task_type = task_types[task["type"]]
    step = task["step"]
    result_count = db[cfg.col_result].count_documents({"task_id": task_id})
    if result_count >= task["responses_needed"]:
        # Send to appropriate aggregator
        aggregator_queue = {
            "form" : cfg.mq_aggregator_form,
            "xml"  : cfg.mq_aggregator_xml
        }[task_type.steps[step]["result_type"]]

        aggregator_message = {
            'task_id': task_id
        }

        print(f"Sent message to {aggregator_queue}: {aggregator_message}")
        send_message(aggregator_message, aggregator_queue, channel)


# Creates all tasks that can be created at the time
# Also create corresponding batches
def create_tasks_and_batches(message, channel):
    score_name = message["name"]
    print(f"Creating tasks for score {score_name}")

    # Get all task types that should be processed now
    eligible_task_types = [t for t in task_types.values() if t.can_execute(score_name) and not t.is_complete(score_name)]
    print(f"- Eligible task types at this point: {eligible_task_types}")

    for task_type in eligible_task_types:
        print(" ", f"Creating tasks for {task_type}")
        task_batches = {}
        # Retrieve slices for given score and task type and create the tasks
        relevant_slices = db[cfg.col_slice].find(task_type.get_slice_query(score_name))
        for measure_slice in relevant_slices:
            slice_location = str(fsm.get_sheet_slices_directory(measure_slice['score']) / task_type.slice_type / measure_slice['name'])
            copy_destination = str(fsm.get_sheet_api_directory(measure_slice['score'], slice_type=task_type.slice_type) / measure_slice['name'])
            first_step = next(iter(task_type.steps.keys()))
            task = {
                'name': measure_slice['name'],
                'type': task_type.name,
                'score': measure_slice['score'],
                'slice_id': str(measure_slice['_id']),
                'image_path': slice_location,
                'step': first_step,
                'xml': getXMLofSlice(measure_slice['score'], measure_slice['start'], measure_slice['end']),
                'responses_needed': task_type.steps[first_step]["min_responses"]
            }

            key = {
                'slice_id': str(measure_slice['_id']),
                'type': task_type.name,
                'score': measure_slice['score']
            }

            entry = db[cfg.col_task].replace_one(key, task, True)

            task_id = entry.upserted_id
            if not task_id:
                task_id = db[cfg.col_task].find_one(key)["_id"]

            batch_key = task_type.get_task_priority(task_id, db)
            if batch_key not in task_batches:
                task_batches[batch_key] = []
            task_batches[batch_key].append(str(task_id))

            copyfile(slice_location, copy_destination)

            if entry.upserted_id:
                print(" ", " ", f"Created task {task} with id {entry.upserted_id}")
            else:
                # TODO: Might need to raise exception as this is undesired behaviour
                print(" ", " ", f"Did nothing, task {task} already exists under id {task_id}")            

        # For each of the task types we would also like to create the necessary batches
        for batch in task_batches:
            batch_dict = {
                "priority": batch,
                "task_type": task_type.name,
                "tasks" : task_batches[batch],
                "submitted": False
            }
            db[cfg.col_task_batch].insert_one(batch_dict)

if __name__ == "__main__":
    # Connect to db
    mongo_client = MongoClient(*cfg.mongodb_address)
    db = mongo_client[cfg.db_name]

    # Initialize task types
    task_types = init_task_types(db)

    # Refresh/add them to DB for task front-end
    for task_type in task_types:
        db[cfg.col_task_type].drop()
        db[cfg.col_task_type].insert_one(task_types[task_type].to_db_dict())

    # Connect to mq and set up callbacks
    parameters = pika.ConnectionParameters(*cfg.rabbitmq_address)
    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()
    channel.queue_declare(queue=cfg.mq_task_scheduler)
    channel.queue_declare(queue=cfg.mq_task_scheduler_status)
    channel.basic_consume(
        on_message_callback=take_action, 
        queue=cfg.mq_task_scheduler, 
        auto_ack=True
    )
    channel.basic_consume(
        on_message_callback=take_action_on_status, 
        queue=cfg.mq_task_scheduler_status, 
        auto_ack=True
    )

    print('Task scheduler 2.0 is listening...')
    channel.start_consuming()
