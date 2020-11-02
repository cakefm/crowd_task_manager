import pika
import yaml
import json
import sys
sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
from pymongo import MongoClient
from bson.objectid import ObjectId
from task_type import init_task_types



if __name__ == "__main__":
    # Connect to db
    # rabbitmq_address = cfg.rabbitmq_address
    client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
    db = client[cfg.db_name]

    task_types = init_task_types(db)

    

    print(task_types["task3"].can_execute())