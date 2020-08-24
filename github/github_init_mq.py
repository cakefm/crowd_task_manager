import sys
import pygit2
import shutil
import pika
import json
import gc

from github import Github
from pymongo import MongoClient

sys.path.append("..")
import common.settings as settings
import common.file_system_manager as fsm
from github_common import commit, push



def callback(ch, method, properties, body):
    data = json.loads(body)
    sheet_name = data['name']

    # Get sheet id
    client = MongoClient(cfg.mongo_address.ip, cfg.mongo_address.port)
    db = client.trompa_test
    sheet_id = str(db[cfg.col_sheet].find_one({"name" : sheet_name})["_id"])

    # Github
    github = Github(cfg.github_token)
    org = github.get_organization(cfg.github_organization_name)
    repo = org.create_repo(sheet_name, description=f"Repository for {sheet_name}", auto_init=True)

    # Git
    git_dir_path = fsm.get_clean_sheet_git_directory(sheet_name)
    clone = pygit2.clone_repository(repo.clone_url, str(git_dir_path))
    clone.remotes.set_url("origin", repo.clone_url)

    # Add the PDF
    pdf_path = fsm.get_sheet_whole_directory(sheet_name) / (sheet_name + ".pdf")
    shutil.copy(str(pdf_path), str(fsm.get_sheet_git_directory(sheet_name)))
    commit(clone, "Initialize master branch")
    push(clone)

    # Add the MEI
    clone.create_branch(cfg.github_branch_name, clone.head.peel())
    branch = clone.lookup_branch(cfg.github_branch_name)
    ref = clone.lookup_reference(branch.name)
    clone.checkout(ref)

    mei_path = fsm.get_sheet_whole_directory(sheet_name) / "aligned.mei"
    shutil.copy(str(mei_path), str(fsm.get_sheet_git_directory(sheet_name)))
    commit(clone, "Initialize crowd manager branch", branch=cfg.github_branch_name)
    push(clone, branch=cfg.github_branch_name)

    # Protect the newly created/pushed branch and the master branch on Github
    repo.get_branch("master").edit_protection(user_push_restrictions=[cfg.github_user])
    repo.get_branch(cfg.github_branch_name).edit_protection(user_push_restrictions=[cfg.github_user])
    
    # Clean up (needed since pygit2 tends to leave files in .git open)
    del clone
    del branch
    del ref
    gc.collect()

    # Update status
    status_update_msg = {
    '_id': sheet_id,
    'module': 'github_init',
    'status': 'complete',
    'name': sheet_name
    }

    global channel
    channel.queue_declare(queue="status_queue")
    channel.basic_publish(exchange="", routing_key="status_queue", body=json.dumps(status_update_msg))


address = cfg.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address.ip, address.port))
channel = connection.channel()
channel.queue_declare(queue=cfg.mq_github_init)
channel.basic_consume(queue=cfg.mq_github_init, on_message_callback=callback, auto_ack=True)
print('Github repository intialization module is listening...')
channel.start_consuming()
