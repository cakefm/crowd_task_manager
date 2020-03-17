VALID_TASK_TYPES = {"transcription", "find", "fix", "verify"}


class TaskProfile:
    def __init__(self, project=str, task_name=str, task_type=str, priority,
                 segment_size):
        self.project = project
        self.task_name = task_name
        self.task_type = task_type
        self.segment_size = segment_size
        self.priority = 0
        self.difficulty = 0
        self.data_input = None
        self.data_output = None
        self.ui_tools = []
        selft.task_url = ""
        self.description = ""
        self.assess_difficulty()
        self.get_url()
