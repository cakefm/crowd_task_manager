import yaml
import re

from pathlib import Path

'''
Contains the required settings for the python scripts from the yaml along with some pre-processing
'''

with open("../settings.yaml", "r") as file:
    config = yaml.safe_load(file.read())

base_sheet_path = Path(config["base_sheet_path"]) # Can just use the "/" operator with Path
mongo_address = re.search('([a-zA-Z]|[0-9]|\.)+:[0-9]+', config["mongo_server"]).group(0).split(":") # [ip, port]
rabbitmq_address = config["rabbitmq_address"].split(":") # [ip, port] 
score_queue_name = config["mq_score_queue"]
sheet_queue_name = config["mq_sheet_queue"]
new_item_queue_name = config["mq_new_item_queue"]
sheet_collection_name = config["mongo_sheet_collection"]
score_collection_name = config["mongo_score_collection"]
slice_collection_name = config["mongo_slice_collection"]
aggregated_result_collection_name = config["mongo_aggregated_result_collection"]
score_rebuilder_queue_name = config["mq_score_rebuilder_queue"]
github_user = config["github_user"]
github_token = config["github_token"]
github_organization_name = config["github_organization"]
github_queue_name = config["mq_github_queue"]
github_init_queue_name = config["mq_github_init_queue"]
github_branch_name = config["github_branch"]
github_commit_count_before_push = int(config["github_commit_count_before_push"])
task_collection_name = config["mongo_task_collection"]
result_collection_name = config["mongo_result_collection"]
aggregator_xml_queue_name = config["mq_aggregator_xml_queue"]
aggregator_form_queue_name = config["mq_aggregator_form_queue"]
omr_planner_status_queue_name = config["mq_omr_planner_status_queue"]
aligner_queue_name = config["mq_aligner_queue"]
task_status_queue_name = config["mq_task_status_queue"]

aggregator_xml_threshold = min(1., max(0., float(config["aggregator_xml_threshold"])))
aggregator_form_threshold = min(1., max(0., float(config["aggregator_form_threshold"])))