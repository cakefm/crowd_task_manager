import pika
import pathlib
import os
import json
import sys

sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm

from slicer import Score, Slice
from pymongo import MongoClient


address = cfg.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address.ip, address.port))
channel = connection.channel()
channel.queue_declare(queue=cfg.mq_slicer)

def callback(ch, method, properties, body):
    data = json.loads(body)
    name = data['name']

    print(f"Processing score {name}")

    path = fsm.get_sheet_base_directory(name)
    score = Score(str(path))

    out_path = fsm.get_sheet_slices_directory(name)
    measure_path = out_path / "measures"
    line_path = out_path / "lines" 
    double_measure_path = out_path / "double_measures"

    slice_paths_lists = {
        measure_path            : score.get_measure_slices(),
        double_measure_path     : score.get_measure_slices(2),
        line_path               : score.get_line_slices()
    }

    client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
    db = client[cfg.db_name]

    for slice_path, slice_list in slice_paths_lists.items():
        pathlib.Path(slice_path).mkdir(parents=True, exist_ok=True)
        for score_slice in slice_list:
            if score_slice.same_page:
                score_slice.get_image().save(str(slice_path / score_slice.get_name()))
                slice_res = db[cfg.col_slice].insert_one(score_slice.to_db_dict())
                print(f"added entry {slice_res.inserted_id} to slices collection")

    channel.queue_declare(queue = cfg.mq_omr_planner_status)

    score_res = db[cfg.col_score].insert_one(score.to_db_dict())
    print(f"added entry {score_res.inserted_id} to scores collection")

    status_update_msg = {
        '_id': data['_id'],
        'module': 'slicer',
        'status': 'complete',
        'name': name}

    channel.basic_publish(exchange='',
        routing_key=cfg.mq_omr_planner_status,
        body=json.dumps(status_update_msg))
    print(f"Published processed score {score.name} to message queue!")

channel.basic_consume(queue=cfg.mq_slicer, on_message_callback=callback, auto_ack=True)

print('Score slicer is listening...')
channel.start_consuming()