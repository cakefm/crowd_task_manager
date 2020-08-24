import sys
import pygit2
import shutil
import pika
import json
import gc

from github import Github
from pymongo import MongoClient
from bson.objectid import ObjectId

sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
from github_common import commit, push


def callback(ch, method, properties, body):
    data = json.loads(body)
    sheet_name = data['name']
    task_id = data['task_id']

    # Get sheet id
    client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
    db = client[cfg.db_name]
    sheet_id = str(db[cfg.col_sheet].find_one({"name" : sheet_name})["_id"])

    # Get task name
    task_name = db[cfg.col_task].find_one(ObjectId(task_id))["name"]

    # Github
    github = Github(cfg.github_token)
    org = github.get_organization(cfg.github_organization_name)
    repo = org.get_repo(sheet_name)

    # Git
    git_dir_path = fsm.get_sheet_git_directory(sheet_name)
    clone = pygit2.Repository(str(git_dir_path))

    # CAUTION: The assumption is that NOONE ever edits the crowd manager's branch except for the crowdmanager itself
    # Thus no need to deal with fast-forwarding or merge conflicts
    clone.remotes[0].fetch()
    branch = clone.lookup_branch(cfg.github_branch_name)
    ref = clone.lookup_reference(branch.name)
    clone.checkout(ref)

    # Copy over new MEI
    mei_path = fsm.get_sheet_whole_directory(sheet_name) / "aligned.mei"
    shutil.copy(str(mei_path), str(fsm.get_sheet_git_directory(sheet_name)))
    commit(clone, f"Update MEI with results from task {task_name}", branch=cfg.github_branch_name)

    # Only push when we have sufficient commits
    global commit_counter
    commit_counter += 1
    if commit_counter >= cfg.github_commit_count_before_push:
        push(clone, branch=cfg.github_branch_name)
        commit_counter = 0

    # Clean up (needed since pygit2 tends to leave files in .git open)
    del clone
    del branch
    del ref
    gc.collect()

    # Update status
    status_update_msg = {
    '_id': sheet_id,
    'module': 'github_update',
    'status': 'complete',
    'name': sheet_name
    }

    global channel
    channel.queue_declare(queue="omr_planner_status_queue")
    channel.basic_publish(exchange="", routing_key="omr_planner_status_queue", body=json.dumps(status_update_msg))


commit_counter = 0
address = cfg.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address.ip, address.port))
channel = connection.channel()
channel.queue_declare(queue=cfg.mq_github)
channel.basic_consume(queue=cfg.mq_github, on_message_callback=callback, auto_ack=True)
print('Github repository update module is listening...')
channel.start_consuming()
