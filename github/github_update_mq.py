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
    action = data['action']

    # Get sheet id
    client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
    db = client[cfg.db_name]
    sheet_id = str(db[cfg.col_sheet].find_one({"name" : sheet_name})["_id"])

    # Get task info
    task = db[cfg.col_task].find_one(ObjectId(task_id))
    task_name = task["name"]
    task_type = task["type"]

    # # Github
    # github = Github(cfg.github_token)
    # org = github.get_organization(cfg.github_organization_name)
    # repo = org.get_repo(sheet_name)

    # Git
    git_dir_path = fsm.get_sheet_git_directory(sheet_name)
    clone = pygit2.Repository(str(git_dir_path))

    # CAUTION: The assumption is that NOONE ever edits the crowd manager's branch except for the crowdmanager itself
    # Thus no need to deal with fast-forwarding or merge conflicts
    clone.remotes[0].fetch()
    branch = clone.lookup_branch(cfg.github_branch)
    ref = clone.lookup_reference(branch.name)
    clone.checkout(ref)

    if action=="commit":
        mei_data = data['mei']
        changed = True
        git_mei_path = fsm.get_sheet_git_directory(sheet_name) / "aligned.mei"

        with open(str(git_mei_path), 'r') as mei_file:
            if mei_file.read() == mei_data:
                changed = False

        if (cfg.only_commit_if_changed and changed) or not cfg.only_commit_if_changed:
            # Copy over new MEI
            with open(str(git_mei_path), 'w') as mei_file:
                mei_file.write(mei_data)
            commit(clone, f"Update MEI with task {task_name} of type {task_type}", branch=cfg.github_branch)
            print(f"Made commit to repo for task {task_id}")
        else:
            print(f"No commit/write made, task {task_id} had no changes")
    elif action=="push":
        push(clone, branch=cfg.github_branch)
        print(f"Made push to repo for task {task_id}")

    # Clean up (needed since pygit2 tends to leave files in .git open)
    del clone
    del branch
    del ref
    gc.collect()

    # Update status
    status_update_msg = {
        '_id': task_id,
        'module': 'github_update',
        'status': 'complete',
        'name': sheet_name
    }

    global channel
    channel.queue_declare(queue=cfg.mq_task_scheduler_status)
    channel.basic_publish(exchange="", routing_key=cfg.mq_task_scheduler_status, body=json.dumps(status_update_msg))


commit_counter = 0
address = cfg.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address.ip, address.port))
channel = connection.channel()
channel.queue_declare(queue=cfg.mq_github)
channel.basic_consume(queue=cfg.mq_github, on_message_callback=callback, auto_ack=True)
print('Github repository update module is listening...')
channel.start_consuming()
