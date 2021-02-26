import pika
import yaml
import json
import sys
import os
import shutil
sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
import common.tree_tools as tt
from pymongo import MongoClient
import pymongo
from bson.objectid import ObjectId
from shutil import copyfile
from task_type import TaskType, Stage
from task_type import DONE_STEP
import xml.dom.minidom as xml


# TODO: maybe move this away from here, might need to be a method on a slice
# TODO: Potentially we always need to start from n=1 to make verovio's renderer happy
#       But we need to keep track of the "n" values to make score rebuilding happen correctly
def get_slice_context_and_xml(measure_slice):
    score_name = measure_slice["score"]
    start = measure_slice['start']
    end = measure_slice['end']
    staff_range = [str(x) for x in range(
        measure_slice['staff_start'] + 1,
        measure_slice['staff_end'] + 1
    )]

    score = db[cfg.col_score].find_one({"name": score_name})

    section = tt.create_element_node("section")

    for measure in score["measures"][start:end]:
        score_def = measure["score_def_before_measure"]
        if score_def:
            section.appendChild(xml.parseString(score_def).documentElement)
        section.appendChild(xml.parseString(measure["xml"]).documentElement)

    context = xml.parseString(measure["context"]).documentElement

    for node in list(section.childNodes) + context.getElementsByTagName('scoreDef'):
        staffs = list(node.getElementsByTagName("staff")) + list(node.getElementsByTagName("staffDef"))
        for staff in staffs:
            if staff.getAttribute("n") not in staff_range:
                tt.delete_node(staff)

    return section.toxml(), context.toxml()


def update_task_url(task_id):
    task = get_task(task_id)
    url = f"{cfg.current_server}{task['type']}/{task['step']}/{task_id}"
    db[cfg.col_task].update_one({"_id": ObjectId(task_id)}, {"$set": {"url": url}})


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
        "start_next_stage": [initialize_stage]
    }[action]

    for action in actions:
        action(message, channel)


def get_task(task_id):
    return db[cfg.col_task].find_one({"_id": ObjectId(task_id)})


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

    print(f"from {message['module']}: {message}")

    # TODO: should probably get rid of the "DONE_STEP" idea somehow, it has too much potential to break things,
    #       maybe use a different field in the task
    task_id = message["_id"]
    task = get_task(task_id)

    # Can happen at the end of a stage, when tasks get wiped --> reconsider wiping of tasks
    if not task:
        print("Task did not exist:", task_id)
        return

    whitelist = ["task_scheduler", "github_update"]
    if task["step"] == DONE_STEP and module not in whitelist:
        print(f"WARNING: Got message after {task_id} was done already from {module}, cancelling")
        # Might be redundant to return here..
        return

    # TODO: rewrite back to if/else again, this is hell for debugging
    # TODO: split "checks" from "actions", makes code more readable and predictable and allows for more control flow
    actions = {
        ("api", None):                              [
                                                        send_to_aggregator
                                                    ],
        ("aggregator_xml",  "complete"):            [
                                                        rebuild_score,
                                                        update_task_xml
                                                    ],
        ("aggregator_xml",  "failed"):              [
                                                        increment_responses_needed,
                                                        resubmit
                                                    ],
        ("score_rebuilder", "complete"):            [
                                                        github_commit,
                                                        increment_step,
                                                        resubmit
                                                    ],
        # Score rebuilding fails only happen at the "done" step for now, so we can ignore them
        ("score_rebuilder", "failed"):              [
                                                    ],
        ("aggregator_form", "complete"):            [
                                                        process_form_output
                                                    ],
        ("aggregator_form", "failed"):              [
                                                        increment_responses_needed,
                                                        resubmit
                                                    ],
        ("form_processor", "verification-passed"):  [
                                                        increment_step,
                                                        resubmit
                                                    ],
        ("form_processor", "verification-failed"):  [
                                                        invalidate_task_results,
                                                        reset_step,  # Reset the entire task for now
                                                        update_task_xml,
                                                        resubmit
                                                    ],
        ("task_scheduler", "complete"):             [
                                                        submit_ce_task_completed,
                                                        submit_next_batch,
                                                        check_task_type_completion,
                                                        check_stage_completion
                                                    ],
        ("github_update", "complete"):              [
                                                    ]
    }[(module, status)]

    for action in actions:
        # This print was spammy
        # print(f"Executing {action} triggered by {message}")
        action(message, channel)


def submit_ce_task_completed(message, channel):
    task_id = message["_id"]
    task = get_task(task_id)

    send_message(
        {
            "action": "task completed",
            "task_id": task_id,
            "task_type": task["type"]
        },
        cfg.mq_ce_communicator,
        channel
    )


def republish_ce_task(task, channel):
    send_message(
        {
            "action": "task completed",
            "task_id": str(task["_id"]),
            "task_type": task["type"]
        },
        cfg.mq_ce_communicator,
        channel
    )
    send_message(
        {
            "action": "task created",
            "task_id": str(task["_id"]),
            "task_type": task["type"]
        },
        cfg.mq_ce_communicator,
        channel
    )


def increment_responses_needed(message, channel):
    task_id = message["_id"]
    task = get_task(task_id)
    new_responses_needed = task["responses_needed"] + 1
    db[cfg.col_task].update_one({"_id": ObjectId(task_id)}, {"$set": {"responses_needed": new_responses_needed}})


def github_commit(message, channel):
    task_id = message["_id"]
    task = get_task(task_id)
    score_name = task["score"]
    send_message(
        {
            'task_id': task_id,
            'action': 'commit',
            'mei': fsm.get_mei_contents(score_name), # We need to do this, or otherwise the commits won't sync up properly with the rest of the system
            'name': score_name
        },
        cfg.mq_github,
        channel
    )


def update_task_xml(message, channel):
    task_id = message["_id"]
    task = get_task(task_id)
    step = task["step"]

    # Should retrieve XML since it only gets called when the task is still in the edit step
    # NOTE: aggregated result is never deleted, only updated. This will retrieve that result if a task
    #       gets reset to another step
    current_result = db[cfg.col_aggregated_result].find_one({"task_id": task_id, "step": step})
    if current_result:
        print(f"Updating task {task_id} XML with currently aggregated result from step {step}")
        new_xml = current_result['result']
    else:
        print(f"No aggregated result found, resetting to initial XML")
        new_xml = task["initial_xml"]

    db[cfg.col_task].update_one({"_id": ObjectId(task_id)}, {"$set": {"xml": new_xml}})


def check_stage_completion(message, channel):
    task_id = message["_id"]
    task = get_task(task_id)
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

# Simply sends a message to the score rebuilder
def rebuild_score(message, channel):
    task_id = message["_id"]
    task = get_task(task_id)
    score_name = task["score"]
    send_message(
        {
            'task_id': message["_id"],
            'name': score_name
        },
        cfg.mq_score_rebuilder,
        channel
    )


# Simply sends a message to the form processor
def process_form_output(message, channel):
    task_id = message["_id"]
    task = get_task(task_id)
    score_name = task["score"]
    send_message(
        {
            'task_id': message["_id"],
            'name': score_name
        },
        cfg.mq_form_processor,
        channel
    )


def check_task_type_completion(message, channel):
    task_id = message["_id"]
    task = get_task(task_id)
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
    task = get_task(task_id)
    task_type = task_types[task["type"]]
    score_name = task["score"]

    batch = db[cfg.col_task_batch].find_one({"_id": task["batch_id"]})
    task_ids = [ObjectId(t) for t in batch["tasks"]]
    tasks = list(db[cfg.col_task].find({'_id': {"$in": task_ids}}))

    if all([t["step"] == DONE_STEP for t in tasks]):
        print(f"Batch {batch['_id']} complete, sending push message to github update..")

        # Push per completed batch
        send_message(
            {
                'task_id': task_id,
                'action': 'push',
                'name': score_name
            },
            cfg.mq_github,
            channel
        )
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
            {
                "action": "task created",
                "task_id": task_id,
                "task_type": batch["task_type"]
            },
            cfg.mq_ce_communicator,
            channel
        )
        send_message({"task_id": task_id}, cfg.mq_task_passthrough, channel)


def invalidate_task_results(message, channel):
    task_id = message["_id"]
    db[cfg.col_result].delete_many({"task_id": task_id})


# TODO: should generalize this eventually, to be able to go to any step by name
def decrement_step(message, channel):
    task_id = message["_id"]
    task = get_task(task_id)
    task_type = task_types[task["type"]]
    current_step = task["step"]

    iterator = iter(reversed(task_type.steps))
    for step in iterator:
        if step == current_step:
            break
    try:
        previous_step = next(iterator)
    except StopIteration:
        previous_step = task_type.steps[0]

    print(f"Decrementing step from {current_step} to {previous_step} for task {task_id}")
    db[cfg.col_task].update_one({"_id": ObjectId(task_id)}, {"$set": {"step": previous_step}})
    update_task_url(task_id)


def reset_step(message, channel):
    task_id = message["_id"]
    task = get_task(task_id)
    task_type = task_types[task["type"]]
    current_step = task["step"]

    print(f"Resetting from step {current_step} to step {task_type.steps[0]} for task {task_id}")
    db[cfg.col_task].update_one({"_id": ObjectId(task_id)}, {"$set": {"step": task_type.steps[0]}})
    update_task_url(task_id)


def increment_step(message, channel):
    task_id = message["_id"]
    task = get_task(task_id)
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
    update_task_url(task_id)


def resubmit(message, channel):
    task_id = message["_id"]
    task = get_task(task_id)
    current_step = task["step"]

    if current_step != DONE_STEP:
        print(f"Resubmitting task {task_id} for step {current_step}")

        # Have to think of the implications for the CE here, probably can just keep the task alive
        republish_ce_task(task, channel)

        send_message({"task_id": task_id}, cfg.mq_task_passthrough, channel)
    else:
        # A bit weird, but this is the most convenient solution: send a message to ourselves!
        print(f"Task {task_id} already done, no need to resubmit, notifying task scheduler..")
        send_message(
            {
                '_id': task_id,
                'module': 'task_scheduler',
                'status': 'complete'
            },
            cfg.mq_task_scheduler_status,
            channel
        )


def send_to_aggregator(message, channel):
    # what we do here:
    # - get the identifier of the task ("identifier")
    # - find all results so far for that task
    # - if it equals or exceeds minimum required responses, send a message to jeboi aggregator (either form or xml)
    # - when the aggregator manages to find a good enough aggregation of results, a different message should advance the step for tasks
    task_id = message["_id"]
    task = get_task(task_id)
    task_type = task_types[task["type"]]
    step = task["step"]
    result_ids = tuple([x["_id"] for x in db[cfg.col_result].find({"task_id": task_id, "step": step})])
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
        if message_key not in message_history and step in task_type.steps:
            # Send to appropriate aggregator
            aggregator_queue = {
                "form": cfg.mq_aggregator_form,
                "xml": cfg.mq_aggregator_xml
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

    # Purge all old batches and tasks, clean up message history
    #
    # TODO: Consider whether this is a good idea, potentially we need to clear
    # more collections, or not clear these at all
    db[cfg.col_task].drop()
    db[cfg.col_task_batch].drop()
    message_history.clear()

    # Send message for task types to CE
    for task_type in task_types:
        send_message(
            {
                "action": 'task type created',
                "score_name": score_name,
                "name": task_type,
                "title": task_type
            },
            cfg.mq_ce_communicator,
            channel
        )

    print(f"Initializing stage {stage.order} for score {score_name}")
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


def copydir(from_path, to_path):
    if os.path.exists(to_path):
        shutil.rmtree(to_path)
    shutil.copytree(from_path, to_path)


def create_tasks_and_batches_for_stage(stage, score_name):
    slices_path = fsm.get_sheet_slices_directory(score_name)
    for task_type in stage.task_types:
        print(" ", f"Creating tasks for {task_type}")
        # Retrieve slices for given score and task type and create the tasks
        relevant_slices = db[cfg.col_slice].find(task_type.get_slice_query(score_name))

        step_size = 200
        tasks = []
        all_tasks = []
        task_ids = []
        for i, measure_slice in enumerate(relevant_slices):
            slice_location = str(slices_path / task_type.slice_type / measure_slice['name'])
            first_step = next(iter(task_type.steps.keys()))
            step_submission = {s: False for s in task_type.steps.keys()}
            xml, context = get_slice_context_and_xml(measure_slice)
            task = {
                'name': measure_slice['name'],
                'type': task_type.name,
                'score': measure_slice['score'],
                'slice_id': str(measure_slice['_id']),
                'page': str(measure_slice['page']),
                'image_path': slice_location,
                'step_submission': step_submission,
                'step': first_step,
                'initial_xml': xml,
                'xml': xml,
                'url': None,  # Gets assigned later
                'context': context,
                'responses_needed': task_type.steps[first_step]["min_responses"],
                'batch_id': None,  # Gets assigned later,
                'stage': stage.order
            }
            tasks.append(task)
            all_tasks.append(task)

            if (len(tasks) >= step_size):
                print(f"Creating tasks {i} to {i + step_size}")
                entries = db[cfg.col_task].insert_many(tasks)
                print("Created tasks: ", entries.inserted_ids)
                task_ids += entries.inserted_ids
                connection.process_data_events()
                tasks = []
        print(f"Creating final tasks")
        entries = db[cfg.col_task].insert_many(tasks)
        task_ids += entries.inserted_ids
        connection.process_data_events()
        tasks = []
        # print("Created tasks: ", entries.inserted_ids)
        create_batches_from_tasks(task_ids, all_tasks, task_type, score_name)

    # Potentially this is not needed anymore, as the front end already has slice access via docker volume
    # api_slices_path = fsm.get_sheet_api_directory(score_name)
    # copydir(slices_path, api_slices_path)


def create_batches_from_tasks(task_ids, all_tasks, task_type, score_name):
    print("Allocating tasks to batches...")
    # First allocate the tasks over the batches
    task_batches = {}
    for task_id, task in zip(task_ids, all_tasks):
        batch_key = task_type.get_task_priority(task)
        if batch_key not in task_batches:
            task_batches[batch_key] = []
        task_batches[batch_key].append(str(task_id))

    # Then create the corresponding batches in the database
    # Also add a ref to the batch in the tasks
    print("Adding batches to DB")
    for batch_priority in task_batches:
        connection.process_data_events()
        batch_dict = {
            "priority": batch_priority,
            "task_type": task_type.name,
            "score": score_name,
            "tasks": task_batches[batch_priority],
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
        db[cfg.col_task].update_many({"_id": {"$in": [ObjectId(t) for t in task_batches[batch_priority]]}}, [
            {"$set": {
                "batch_id": batch_id,
                "url": {"$concat": [f"{cfg.current_server}{task_type.name}/", "$step", "/", {"$toString": "$_id"}]}
            }}
        ])
        # for task_id in task_batches[batch_priority]:
        #     db[cfg.col_task].update_one({"_id": ObjectId(task_id)}, {"$set": {"batch_id": batch_id}})
        #     update_task_url(str(task_id))


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
    for stage_path in sorted([x for x in fsm.get_task_types_directory().iterdir() if x.is_dir()], key=get_order):
        task_types_stage = set()
        for task_type_path in stage_path.iterdir():
            if (task_type_path.suffix == '.yaml'):
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
