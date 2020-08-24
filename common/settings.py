import yaml

from pathlib import Path
from collections import namedtuple

'''
Contains the required settings for the python scripts from the yaml along with some pre-processing
'''

# Some flags for debugging and convenience
DYNAMIC_REFRESH = False
CHECK_IF_CONFIG_PARSES = True
SEPARATOR = "|"
CFG_PATH = "../settings.yaml"

# Config class
class Cfg(object):
    def __init__(self):
        self.refresh()

        # Here you can add additional parsing rules/types
        IP_address = namedtuple("IP_address", ["ip", "port"])
        self._type_dict = {
            "int": int,
            "float": float,
            "ip": lambda x: IP_address(*x.split(":")),
            "path" : Path
        }

    def refresh(self):
        with open(CFG_PATH, "r") as file:
            self.config = yaml.safe_load(file.read())
            self.names = dict()
            for entry in self.config:
                self.names[entry.split(SEPARATOR)[0]] = entry

    def read_value(self, name):
        if DYNAMIC_REFRESH:
            self.refresh()

        split = self.names[name].split(SEPARATOR)

        name = split[0]
        value = self.config[self.names[name]]

        if len(split) > 1:
            value_type = self._type_dict[split[1]]
            value = value_type(value)

            if len(split) > 2:
                value_range = tuple(map(value_type, split[2].split("...")))
                value = max(value_range[0], min(value, value_range[1]))
        return value

def _create_cfg_property(name):
    return property(lambda self: self.read_value(name))

cfg = Cfg()
for entry in cfg.config:
    name = entry.split(SEPARATOR)[0]
    #self.read_value(name)
    setattr(Cfg, name, _create_cfg_property(name))

if CHECK_IF_CONFIG_PARSES:
    for name in dir(cfg):
        if not name.startswith("__"):
            try:
                getattr(cfg, name)
            except Exception as e:
                raise Exception("Config has parsing errors!") from e



# base_sheet_path = Path(config["base_sheet_path"]) # Can just use the "/" operator with Path
# mongo_address = re.search('([a-zA-Z]|[0-9]|\.)+:[0-9]+', config["mongo_server"]).group(0).split(":") # [ip, port]
# rabbitmq_address = config["rabbitmq_address"].split(":") # [ip, port] 
# score_queue_name = config["mq_score_queue"]
# sheet_queue_name = config["mq_sheet_queue"]
# new_item_queue_name = config["mq_new_item_queue"]
# sheet_collection_name = config["mongo_sheet_collection"]
# score_collection_name = config["mongo_score_collection"]
# slice_collection_name = config["mongo_slice_collection"]
# aggregated_result_collection_name = config["mongo_aggregated_result_collection"]
# score_rebuilder_queue_name = config["mq_score_rebuilder_queue"]
# github_user = config["github_user"]
# github_token = config["github_token"]
# github_organization_name = config["github_organization"]
# github_queue_name = config["mq_github_queue"]
# github_init_queue_name = config["mq_github_init_queue"]
# github_branch_name = config["github_branch"]
# github_commit_count_before_push = int(config["github_commit_count_before_push"])
# task_collection_name = config["mongo_task_collection"]
# result_collection_name = config["mongo_result_collection"]
# aggregator_xml_queue_name = config["mq_aggregator_xml_queue"]
# aggregator_form_queue_name = config["mq_aggregator_form_queue"]
# omr_planner_status_queue_name = config["mq_omr_planner_status_queue"]
# aligner_queue_name = config["mq_aligner_queue"]
# task_status_queue_name = config["mq_task_status_queue"]

# aggregator_xml_threshold = min(1., max(0., float(config["aggregator_xml_threshold"])))
# aggregator_form_threshold = min(1., max(0., float(config["aggregator_form_threshold"])))