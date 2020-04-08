import yaml
import re

from pathlib import Path

'''
Contains the required settings for the python scripts from the yaml along with some pre-processing
'''
_config = {}
def reload():
    with open("../settings.yaml", "r") as file:
        global _config
        _config = yaml.safe_load(file.read())
reload()

class ConfigParseError(Exception):
    pass

# "lenient" is a temporary solution to make debugging easier.
# Long-term solution: only import settings that are needed at the locations they are used --> need to turn them into lamdbas
def _parse(entry, f=lambda x : x, default="", lenient=False):
    try:
        entry_type = type(default)
        return entry_type(f(_config[entry])) # Use constructor of entry type
    except Exception as e:
        if lenient:
            print(f"WARNING: Could not parse config entry '{entry}', ensure it has the correct format, continuing with default value: {default}")
        else:
            raise ConfigParseError(f"ERROR: Could not parse config entry '{entry}', ensure it has the correct format (set 'lenient' to `True` to avoid this).", e) from e
    return default



base_sheet_path                     = _parse("base_sheet_path", default=Path("sheets"))
mongo_address                       = _parse("mongo_server", f=lambda x : re.search('[a-zA-Z]+:[0-9]+', x).group(0).split(":")) 
rabbitmq_address                    = _parse("rabbitmq_address", f=lambda x: x.split(":")) # [ip, port] 
score_queue_name                    = _parse("mq_score_queue")
sheet_queue_name                    = _parse("mq_sheet_queue")
new_item_queue_name                 = _parse("mq_new_item_queue")
sheet_collection_name               = _parse("mongo_sheet_collection")
score_collection_name               = _parse("mongo_score_collection")
slice_collection_name               = _parse("mongo_slice_collection")
aggregated_result_collection_name   = _parse("mongo_aggregated_result_collection")
score_rebuilder_queue_name          = _parse("mq_score_rebuilder_queue")
github_user                         = _parse("github_user")
github_token                        = _parse("github_token")
github_organization_name            = _parse("github_organization")
github_queue_name                   = _parse("mq_github_queue")
github_init_queue_name              = _parse("mq_github_init_queue")
github_branch_name                  = _parse("github_branch")
github_commit_count_before_push     = _parse("github_commit_count_before_push", default=5)
task_collection_name                = _parse("mongo_task_collection")