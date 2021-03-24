import sys
import pygit2
import shutil
import pika
import json
import gc
import time

from github import Github
from github import GithubException
from pymongo import MongoClient

sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
from github_common import commit, push

def callback(ch, method, properties, body):
    data = json.loads(body)
    sheet_name = data['name']

    # Get sheet id
    sheet_id = str(db[cfg.col_sheet].find_one({"name" : sheet_name})["_id"])

    # Github
    github = Github(cfg.github_token)
    org = github.get_organization(cfg.github_organization)

    if cfg.delete_if_exists:
        try:
            org.get_repo(sheet_name).delete()
            print("Deleted existing repo for", sheet_name)
        except GithubException as e:
            print("Repo doesn't exist, ready for creation!")
            print(str(e))
        # if "name already exists on this account" in str(e):

    # TODO: Handling this properly requires offline functionality for the git-repo, meaning we have to
    #       create it without relying on Github and then link it if possible
    repo = org.create_repo(sheet_name, description=f"Repository for {sheet_name}", auto_init=True)

    # Git
    git_dir_path = fsm.get_clean_sheet_git_directory(sheet_name)
    clone = None
    tries = 0
    while clone==None and tries < 5:
        try:
            clone = pygit2.clone_repository(repo.clone_url, str(git_dir_path))
        except pygit2.GitError:
            print("Could not clone repo at:", repo.clone_url, ", trying again in 1 second...")
            connection.process_data_events()
            tries += 1
            time.sleep(1)

    status = "complete"
    if clone != None:

        clone.remotes.set_url("origin", repo.clone_url)

        # Add the PDF
        pdf_path = fsm.get_sheet_whole_directory(sheet_name) / (sheet_name + ".pdf")
        shutil.copy(str(pdf_path), str(fsm.get_sheet_git_directory(sheet_name)))
        commit(clone, "Initialize main branch")

        pushed = False
        push_tries = 0
        while not pushed and push_tries < 5:
            try:
                push(clone)
                pushed = True
            except pygit2.GitError:
                print(f"Could not push for score {sheet_name}, retrying in 1 second...")
                connection.process_data_events()
                push_tries += 1
                time.sleep(1)
        if pushed:
            # Add the MEI
            clone.create_branch(cfg.github_branch, clone.head.peel())
            branch = clone.lookup_branch(cfg.github_branch)
            ref = clone.lookup_reference(branch.name)
            clone.checkout(ref)

            mei_path = fsm.get_sheet_whole_directory(sheet_name) / "aligned.mei"
            shutil.copy(str(mei_path), str(fsm.get_sheet_git_directory(sheet_name)))
            commit(clone, "Initialize crowd manager branch", branch=cfg.github_branch)
            pushed_branch = False
            branch_push_tries = 0
            while not pushed_branch and branch_push_tries < 5:
                try:
                    push(clone, branch=cfg.github_branch, force=True)
                    pushed_branch = True
                except pygit2.GitError:
                    print(f"Could not push for score {sheet_name}, retrying in 1 second...")
                    connection.process_data_events()
                    branch_push_tries += 1
                    time.sleep(1)

            if pushed and pushed_branch:
                # Protect the newly created/pushed branch and the main branch on Github
                repo.get_branch("main").edit_protection(user_push_restrictions=[cfg.github_user])
                repo.get_branch(cfg.github_branch).edit_protection(user_push_restrictions=[cfg.github_user])

            if not pushed_branch:
                print("Warning, could not push crowd manager's branch for", sheet_name)
                status = "failed"

            del branch
            del ref
        else:
            print("Warning, could not push initial commit for", sheet_name)
            status = "failed"

        # Clean up (needed since pygit2 tends to leave files in .git open)
        del clone
        gc.collect()
    else:
        print("Warning, could not initialize repo for", sheet_name)
        status = "failed"

    # Update status
    status_update_msg = {
        '_id': sheet_id,
        'module': 'github_init',
        'status': status,
        'name': sheet_name
    }

    global channel
    channel.queue_declare(queue=cfg.mq_omr_planner_status)
    channel.basic_publish(exchange="", routing_key=cfg.mq_omr_planner_status, body=json.dumps(status_update_msg))


address = cfg.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address.ip, address.port))
channel = connection.channel()
channel.queue_declare(queue=cfg.mq_github_init)
channel.basic_consume(queue=cfg.mq_github_init, on_message_callback=callback, auto_ack=True)

client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
db = client[cfg.db_name]

print('Github repository intialization module is listening...')
channel.start_consuming()
