import pika
import yaml
import json
import sys
sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
from pymongo import MongoClient
import pymongo
from bson.objectid import ObjectId
from shutil import copyfile
from task_type import TaskType, Stage
from task_type import DONE_STEP

def getXMLofSlice(score_name, begin, end):
    score = db[cfg.col_score].find_one({"name": score_name})
    merged = "\n".join([m["xml"] for m in score["measures"][begin:end]])
    return f"<placeholderroot>{merged}</placeholderroot>"

def send_message(message, queue, channel):
    json_str = json.dumps(message)
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange="", routing_key=queue, body=json_str)

# Main callback handler, for communication with OMRP among other things
# spec:
#   name    :   name of score
#   action  :   action to take
def take_action(channel, method, properties, body):
    message = json.loads(body)
    action = message['action']

    actions = {
        "start_next_stage"         : [initialize_stage]
    }[action]

    for action in actions:
        action(message, channel)


# Status callback handler
# spec:
#   _id     :   the task id 
#   module  :   module this message was sent from 
#   status  :   optional, either "complete" or "failed"
# TODO: change action arguments to include already retrieved task so we don't have to look it up 10+ times
def take_action_on_status(channel, method, properties, body):
    message = json.loads(body)
    module = message['module']
    # TODO: task status queue spec needs to be cleared up, so we can factor out actions
    status = message.get('status', None)

    def log_status(message, channel):
        print(f"from {message['module']}: {message}")
    
    actions = {
        ("api", None)                           :   [
                                                        log_status, 
                                                        send_to_aggregator
                                                    ],
        ("aggregator_xml",  "complete")         :   [
                                                        log_status, 
                                                        increment_step,
                                                        rebuild_score
                                                    ],
        ("aggregator_xml",  "failed")           :   [
                                                        log_status, 
                                                        resubmit
                                                    ],
        ("aggregator_form", "complete")         :   [
                                                        log_status, 
                                                        increment_step,
                                                        rebuild_score
                                                    ],
        ("score_rebuilder", "complete")         :   [
                                                        log_status,
                                                        resubmit,
                                                        submit_next_batch,
                                                        check_task_type_completion,
                                                        check_stage_completion
                                                    ],
        ("aggregator_form", "failed")           :   [
                                                        log_status, 
                                                        resubmit
                                                    ]
    }[(module, status)]

    for action in actions:
        # print(f"Executing {action} triggered by {message}")
        action(message, channel)


def check_stage_completion(message, channel):
    task_id = message["_id"]
    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})
    stage_order = task["stage"]
    score_name = task["score"]
    score = db[cfg.col_score].find_one({"name": score_name})

    # Check the completion
    current_stage = determine_current_stage(score_name)
    if current_stage.is_complete(score_name):
        print(f"Finished stage {current_stage.order}, notifying OMRP and updating campaign status")

        # Update campaign status for this score
        db[cfg.col_campaign_status].update_one(
            {"score_name": score_name},
            { "$addToSet": { "stages_done": current_stage.order } }
        )

        # If this was the final stage, we'd like to mark the campaign as finished
        if current_stage.order == stages[-1].order:
            print(f"Stage {current_stage.order} was the last stage, campaign is finished!")
            db[cfg.col_campaign_status].update_one(
                {"score_name": score_name},
                {"$set": {"finished": True}}
            )

        # Let OMRP know we're done for now
        send_message(
            {
                '_id': str(score["_id"]),
                'module': 'task_scheduler',
                'status': 'complete',
                'stage': stage_order,  # Note, this is the previous stage
                'name': score_name
            },
            cfg.mq_omr_planner_status,
            channel
        )

        # Purge all batches and tasks, clean up message history
        # TODO: Consider whether this is a good idea, potentially we need to clear
        # more collections, or not clear them at all
        db[cfg.col_task].drop()
        db[cfg.col_task_batch].drop()
        message_history.clear()

# Simply sends a message to the score rebuilder
def rebuild_score(message, channel):
    task_id = message["_id"]
    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})
    score_name = task["score"]
    send_message(
        {
            'task_id': message["_id"],
            'name': score_name
        },
        cfg.mq_score_rebuilder,
        channel
    )        

def check_task_type_completion(message, channel):
    task_id = message["_id"]
    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})
    task_type = task_types[task["type"]]
    score_name = task["score"]
    score = db[cfg.col_score].find_one({"name": score_name})
    campaign_status = db[cfg.col_campaign_status].find_one({"score_name": score_name})

    if task_type.is_complete(score_name) and task_type.name not in campaign_status["task_types_done"]:
        # Update campaign status for this score
        db[cfg.col_campaign_status].update_one(
            {"score_name": score_name},
            {"$addToSet": {"task_types_done": task_type.name}}
        )

        # Let OMRP know we just finished a task type
        send_message(
            {
                '_id': str(score["_id"]),
                'module': 'task_scheduler',
                'status': 'complete',
                'task_type': task_type.name,
                'name': score_name
            },
            cfg.mq_omr_planner_status,
            channel
        )    




def submit_next_batch(message, channel):
    task_id = message["_id"]
    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})
    task_type = task_types[task["type"]]
    score_name = task["score"]

    batch = db[cfg.col_task_batch].find_one({"_id": task["batch_id"]})
    task_ids = [ObjectId(t) for t in batch["tasks"]]
    tasks = list(db[cfg.col_task].find({'_id': {"$in": task_ids}}))

    if all([t["step"] == DONE_STEP for t in tasks]):
        priority = batch["priority"]
        key = {
                "priority": { "$gt": priority},
                "task_type": task_type.name,
                "score": score_name,
                "submitted": False
        }
        next_batch = db[cfg.col_task_batch].find_one(key, sort=[('priority', pymongo.ASCENDING)])
        if next_batch:
            submit_batch(next_batch, channel)

def submit_batch(batch, channel):
    print(f"Submitting batch: {batch}")
    db[cfg.col_task_batch].update_one({"_id": batch["_id"]}, {"$set": {"submitted": True}})
    for task_id in batch["tasks"]:
        send_message(
            {'task_id': task_id},
            cfg.mq_ce_communicator,
            channel
        )
    

def increment_step(message, channel):
    task_id = message["_id"]    
    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})
    task_type = task_types[task["type"]]
    current_step = task["step"]

    iterator = iter(task_type.steps)
    for step in iterator:
        if step == current_step:
            break
    try:
        next_step = next(iterator)
    except StopIteration:
        next_step = DONE_STEP

    print(f"Advancing step from {current_step} to {next_step} for task {task_id}")
    db[cfg.col_task].update_one({"_id": ObjectId(task_id)}, {"$set": {"step": next_step}})

def resubmit(message, channel):
    task_id = message["_id"]    
    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})
    current_step = task["step"]

    if current_step != DONE_STEP:
        print(f"Resubmitting task {task_id} for step {current_step}")
        send_message(
            {'task_id': task_id},
            cfg.mq_ce_communicator,
            channel
        )
    else:
        print(f"Task {task_id} already done, no need to resubmit")


def send_to_aggregator(message, channel):
    # what we do here:
    # - get the identifier of the task ("identifier")
    # - find all results so far for that task
    # - if it equals or exceeds minimum required responses, send a message to jeboi aggregator (either form or xml)
    # - when the aggregator manages to find a good enough aggregation of results, a different message should advance the step for tasks
    task_id = message["_id"]
    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})
    task_type = task_types[task["type"]]
    step = task["step"]
    result_ids = tuple([x["_id"] for x in db[cfg.col_result].find({"task_id": task_id})])
    result_count = len(result_ids)
    print(f"Counted {result_count} results for task {task_id}")
    if result_count >= task["responses_needed"]:
        # We don't want to re-send for exact the same results, this prevents the following race condition:
        # 1.) API gets very quick successive result submissions
        # 2.) On each of these, a message is sent to the task scheduler
        # 3.) Task scheduler will already send to aggregator on the first message
        # 4.) The messages afterwards cause task scheduler to send again while the aggregator had long finished
        # Basically this happens when the aggregator is faster at aggregating than the task_scheduler at processing the queue
        message_key = (task_id, result_ids, "aggregator")
        if message_key not in message_history:
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
            message_history.add(message_key)

# Determine the stage that should be currently worked on and create the required tasks and batches for it
def initialize_stage(message, channel):
    score_name = message["name"]
    stage = determine_current_stage(score_name)
    create_tasks_and_batches_for_stage(stage, score_name)
    send_first_batches(stage, score_name, channel)

# We can send one batch per task type
def send_first_batches(stage, score_name, channel):
    for task_type in stage.task_types:
        key = {
                "task_type": task_type.name,
                "score": score_name,
                "submitted": False
        }
        next_batch = db[cfg.col_task_batch].find_one(key, sort=[('priority', pymongo.ASCENDING)])
        submit_batch(next_batch, channel)

def create_tasks_and_batches_for_stage(stage, score_name):
    for task_type in stage.task_types:
        print(" ", f"Creating tasks for {task_type}")
        # Retrieve slices for given score and task type and create the tasks
        relevant_slices = db[cfg.col_slice].find(task_type.get_slice_query(score_name))
        task_ids = []
        for measure_slice in relevant_slices:
            slice_location = str(fsm.get_sheet_slices_directory(measure_slice['score']) / task_type.slice_type / measure_slice['name'])
            copy_destination = str(fsm.get_sheet_api_directory(measure_slice['score'], slice_type=task_type.slice_type) / measure_slice['name'])
            first_step = next(iter(task_type.steps.keys()))
            step_submission = {s: False for s in task_type.steps.keys()}

            task = {
                'name': measure_slice['name'],
                'type': task_type.name,
                'score': measure_slice['score'],
                'slice_id': str(measure_slice['_id']),
                'image_path': slice_location,
                'step_submission': step_submission,
                'step': first_step,
                'xml': getXMLofSlice(measure_slice['score'], measure_slice['start'], measure_slice['end']),
                'responses_needed': task_type.steps[first_step]["min_responses"],
                'batch_id': None, # Gets assigned later,
                'stage': stage.order
            }

            key = {
                'slice_id': str(measure_slice['_id']),
                'type': task_type.name,
                'score': measure_slice['score']
            }

            # TODO: upsertion may be undesirable: if the task manager crashes, task progress will be "wiped"
            # instead, maybe it is better to not update at all if the task already exists.
            entry = db[cfg.col_task].replace_one(key, task, True)

            task_id = entry.upserted_id
            if not task_id:
                task_id = db[cfg.col_task].find_one(key)["_id"]
            task_ids.append(task_id)

            copyfile(slice_location, copy_destination)

            if entry.upserted_id:
                print(" ", " ", f"Created task {task} with id {entry.upserted_id}")
            else:
                # TODO: Might need to raise exception as this is undesired behaviour
                print(" ", " ", f"Did nothing, task {task} already exists under id {task_id}")

        create_batches_from_tasks(task_ids, task_type, score_name)


def create_batches_from_tasks(task_ids, task_type, score_name):
    # First allocate the tasks over the batches
    task_batches = {}
    for task_id in task_ids:
        batch_key = task_type.get_task_priority(task_id, db)
        if batch_key not in task_batches:
            task_batches[batch_key] = []
        task_batches[batch_key].append(str(task_id))

    # Then create the corresponding batches in the database
    # Also add a ref to the batch in the tasks
    for batch_priority in task_batches:
        batch_dict = {
            "priority": batch_priority,
            "task_type": task_type.name,
            "score": score_name,
            "tasks" : task_batches[batch_priority],
            "submitted": False  # Need this to ensure we don't submit the same batch multiple times
        }

        key = {
            "priority": batch_priority,
            "task_type": task_type.name,
            "score": score_name
        }

        # NOTE: This would wipe the "submitted" state of existing batches in the db
        entry = db[cfg.col_task_batch].replace_one(key, batch_dict, True)
        batch_id = entry.upserted_id if entry.upserted_id else db[cfg.col_task_batch].find_one(key)["_id"]

        for task_id in task_batches[batch_priority]:
            db[cfg.col_task].update_one({"_id": ObjectId(task_id)}, {"$set": {"batch_id": batch_id}})


def determine_current_stage(score_name):
    campaign_status = db[cfg.col_campaign_status].find_one({"score_name": score_name})

    for stage in stages:
        if stage.order not in campaign_status["stages_done"]:
            return stage

# TODO: Preceeding zeroes in folder names may prevent uniqueness of order
# the assumption is that the stage order is unique, so this should be enforced or remapped
def init_task_types_and_stages(db):
    stages = []
    task_types = {}
    prev_stage = None
    get_order = lambda path: int(path.stem.split("_")[0])
    for stage_path in sorted(fsm.get_task_types_directory().iterdir(), key=get_order):
        task_types_stage = set()
        for task_type_path in stage_path.iterdir():
            with open(task_type_path) as f:
                name = f"{stage_path.stem}_{task_type_path.stem}"
                t = yaml.safe_load(f)
                task_type = TaskType(name, t, db)
                task_types[name] = task_type
                task_types_stage.add(task_type)
        stage = Stage(get_order(stage_path), task_types_stage, prev_stage)
        stages.append(stage)
        prev_stage = stage
    return stages, task_types

if __name__ == "__main__":
    # Connect to db
    mongo_client = MongoClient(*cfg.mongodb_address)
    db = mongo_client[cfg.db_name]

    # Initialize task types and stages
    stages, task_types = init_task_types_and_stages(db)

    # Refresh/add them to DB for task front-end
    db[cfg.col_task_type].drop()
    for task_type in task_types:
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

    # Message history, to combat some race condition issues
    # Mostly used to deal with the API/aggregator race condition
    # TODO: A nicer and more formal way of handling this would be cool
    message_history = set()

    print('Task scheduler 2.0 is listening...')
    channel.start_consuming()
