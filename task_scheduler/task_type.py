import yaml
import sys
sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm

import priority_functions

# valid steps
STEPS = {
"edit",
"verify"
}
DONE_STEP = "done"


class TaskType():
    def __init__(self, name, data, db):
        self.name = name
        self.requirements = data["requirements"]
        self.dependencies = set(data["dependencies"])
        self.prioritization = getattr(priority_functions, data["prioritization"])
        self.steps = data["steps"]
        self.slice_type = data["slice_type"]
        self.slice_tuple_size = 0 # matches everything
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
        CFG_PLACEHOLDER = 1
        done = [task_type.get_completion_status(score) >= CFG_PLACEHOLDER for task_type in chain]
        if all(done):
            return True
        return False
    
    def get_slice_query(self, score):
        slice_query = {
            "type": self.name, 
            "score": score,
            "type": self.slice_type
        }
        if self.slice_tuple_size > 0:
            slice_query["tuple_size"] = self.slice_tuple_size
        return slice_query

    def get_completion_status(self, score):
        all_tasks = self.db[cfg.col_task].count_documents({"type": self.name})
        completed_tasks = self.db[cfg.col_task].count_documents({"type": self.name, "status":DONE_STEP})
        return 0 if all_tasks==0 else completed_tasks / all_tasks
        
    def __repr__(self):
        return "<" + ";".join([
            self.name, 
            str(self.requirements), 
            str(list(map(lambda x: x.name, self.dependencies))), 
            str(self.prioritization.__name__), 
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
