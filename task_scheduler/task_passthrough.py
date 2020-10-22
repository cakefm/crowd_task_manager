import sys
sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
import time
import requests

from pymongo import MongoClient

if __name__ == "__main__":
    processed = set()
    client = MongoClient(*cfg.mongodb_address)
    db = client[cfg.db_name]
    task_collection = db[cfg.col_task]
    threshold = 3
    while True:
        print("Polling...")
        # Whenever we find tasks, we pretend we just finished them by creating an API request three times
        # TODO: This "three" times should change into an appropriate config value and then be adjusted here
        for task in task_collection.find():
            if task['_id'] not in processed:
                processed.add(task['_id'])
                payload = task['xml']
                for i in range(threshold):
                    requests.post(f"http://localhost:443/{task['_id']}", data=payload)
                    time.sleep(0.1) # Just to make sure nothing weird happens
                print(f"Passed through task with ID {task['_id']} as result {threshold} times")
        time.sleep(1)