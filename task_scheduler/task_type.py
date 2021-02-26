import sys
sys.path.append("..")
from common.settings import cfg

import priority_functions
from collections import OrderedDict

# valid steps
# TODO: maybe this should be formalized in a nicer way
STEPS = {
"edit",
"verify"
}
DONE_STEP = "done"


class TaskType():
    def __init__(self, name, body, db):
        self.name = name
        self.get_task_priority = getattr(priority_functions, body["prioritization"])
        self.steps = OrderedDict({x["name"]: x for x in body["steps"]})
        self.slice_type = body["slice_type"]
        self.slice_tuple_size = 0  # matches all sizes
        if ":" in self.slice_type:
            self.slice_type, slice_tuple_size = self.slice_type.split(":")
            self.slice_tuple_size = int(slice_tuple_size)
        self.db = db

    def get_slice_query(self, score):
        slice_query = {
            "score": score,
            "type": self.slice_type
        }
        if self.slice_tuple_size > 0:
            slice_query["tuple_size"] = self.slice_tuple_size
        return slice_query

    def is_complete(self, score):
        CFG_PLACEHOLDER = 1
        return self.get_progress(score) >= CFG_PLACEHOLDER

    # Every slice has EXACTLY one task per score/task_type
    def get_progress(self, score):
        required_slices = self.db[cfg.col_slice].count_documents(self.get_slice_query(score))
        completed_tasks = self.db[cfg.col_task].count_documents({"type": self.name, "step": DONE_STEP})
        return 0 if required_slices == 0 else completed_tasks / required_slices

    # Only store information that we might need in other places than the task scheduler
    # we can easily read the task types in anyway and this gets done on startup in task scheduler
    def to_db_dict(self):
        return {
            "name": self.name,
            "steps": dict(self.steps)
        }

    def __repr__(self):
        return "<" + ";".join([
            self.name,
            str(self.get_task_priority.__name__),
            str(self.steps)
            ]) + ">"


class Stage():
    def __init__(self, order, task_types, previous_stage):
        self.order = order
        self.task_types = task_types
        self.previous_stage = previous_stage

    def get_task_type_progress(self, score):
        return {tt.name: tt.get_progress(score) for tt in self.task_types}

    def is_complete(self, score):
        return all([tt.is_complete(score) for tt in self.task_types])

    # Can execute if all previous stages are complete
    # TODO: This is the safe route for now as it prevents data duplication, in the future we may want to cache things
    def can_execute(self, score):
        stage = self.previous_stage
        while stage:
            if not stage.is_complete(score):
                return False
            stage = stage.previous_stage
        return True
