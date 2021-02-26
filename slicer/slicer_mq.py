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
    score = Score(name)

    out_path = fsm.get_sheet_slices_directory(name)
    measure_path = out_path / "measures"
    line_path = out_path / "lines" 
    double_measure_path = out_path / "double_measures"

    client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
    db = client[cfg.db_name]

    # First clear all documents under this score name (which acts as key in our case)
    db[cfg.col_slice].delete_many({"score": score.name})

    # determine staff count from first measure
    staffs = range(len(score.measures[0].staffs))

    print("Getting slices to create...")
    slice_paths_lists = {
        measure_path            : [score.get_measure_slices(staff_start=index, staff_end=index+1) for index in staffs],
        # line_path               : [score.get_line_slices()]
    }

    slices = []
    for slice_path, slice_list_list in slice_paths_lists.items():
        for slice_list in slice_list_list:
            pathlib.Path(slice_path).mkdir(parents=True, exist_ok=True)
            print(f"Creating slice images for {len(slice_list)} slices")
            connection.process_data_events()
            for score_slice in slice_list:
                if score_slice.same_page:
                    score_slice.get_image().save(str(slice_path / score_slice.get_name()))
                    slices.append(score_slice.to_db_dict())
    slice_res = db[cfg.col_slice].insert_many(slices)
    print(f"Added slice entries to db:", slice_res.inserted_ids)

    channel.queue_declare(queue=cfg.mq_omr_planner_status)

    entry = db[cfg.col_score].replace_one({"name": score.name}, score.to_db_dict(), upsert=True)
    if entry.upserted_id:
        print(f"added entry {entry.upserted_id} to scores collection")

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