import sys
sys.path.append("..")
from common.settings import cfg
from bson.objectid import ObjectId

# priority functions get a task_id and db as input, and output a number indicating the priority of the task
def page_order(task):
    return task["page"]