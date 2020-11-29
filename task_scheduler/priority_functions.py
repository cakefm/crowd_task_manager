import sys
sys.path.append("..")
from common.settings import cfg
from bson.objectid import ObjectId

# priority functions get a task_id and db as input, and output a number indicating the priority of the task
def page_order(task_id, db):
    task = db[cfg.col_task].find_one({"_id": ObjectId(task_id)})
    task_slice = db[cfg.col_slice].find_one({"_id": ObjectId(task["slice_id"])})
    score = db[cfg.col_score].find_one({"name": task_slice["score"]})
    first_measure = score["measures"][task_slice["start"]]
    staff = first_measure["staffs"][task_slice["staff"]]
    return staff["page_index"]