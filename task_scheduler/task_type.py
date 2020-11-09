import yaml
import sys
sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm

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
    def __init__(self, name, data, db):
        self.name = name
        self.dependencies = set(data["dependencies"])
        self.get_task_priority = getattr(priority_functions, data["prioritization"])
        self.steps = OrderedDict({x["name"]:x for x in data["steps"]})
        self.slice_type = data["slice_type"]
        self.slice_tuple_size = 0 # matches all sizes
        if ":" in self.slice_type:
            self.slice_type, slice_tuple_size = self.slice_type.split(":")
            self.slice_tuple_size = int(slice_tuple_size)
        self.db = db
    
    # Checks for cycles as well
    def get_dependency_chain(self):
        chain = set()
        to_visit = set() | self.dependencies
        while to_visit:
            next_dependency = to_visit.pop()
            chain.add(next_dependency)
            if next_dependency.dependencies:
                if next_dependency.name in chain:
                    raise Exception("Cycle encountered, check dependencies!")
                to_visit |= next_dependency.dependencies
                
        return chain
    
    # TODO: potentially redundant to test the full chain every time as we could stop at the first incomplete dependency
    # but might be good to keep it like this for additional safety
    def can_execute(self, score):
        chain = self.get_dependency_chain()
        done = [task_type.is_complete(score) for task_type in chain]
        if all(done):
            return True
        return False
    
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
        completed_tasks = self.db[cfg.col_task].count_documents({"type": self.name, "status":DONE_STEP})
        return 0 if required_slices==0 else completed_tasks / required_slices
    
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
            str(list(map(lambda x: x.name, self.dependencies))), 
            str(self.get_task_priority.__name__), 
            str(self.steps)
            ]) + ">"


def init_task_types(db):
    # Initialize our task archetypes
    task_types = {}
    for path in fsm.get_task_types_directory().iterdir():
        with open(path) as f:
            t = yaml.safe_load(f)
            task_types[path.stem] = TaskType(path.stem, t, db)

    # Create references
    for t in task_types:
        task_types[t].dependencies = set(map(task_types.get, task_types[t].dependencies))

    return task_types
