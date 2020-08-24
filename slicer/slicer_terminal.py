import argparse
import pika
import pathlib
import os
import sys

sys.path.append("..")
from common.settings import cfg

from slicer import Score, Slice
from pymongo import MongoClient


parser = argparse.ArgumentParser(description='Get image slices from .mei music scores, given that the images are in the same folder as the .mei file.')
parser.add_argument('path', type=str, help='Path to the main directory of the score')
parser.add_argument('-o','--output', type=str, help='Output path, if not given it will create a folder called "slices" at the same location as the .mei file.')
parser.add_argument('-s','--store_in_db', action="store_true", help="Store whatever slices are being created in mongo db, uses address and collection names from config.")
parser.add_argument('-q','--message_queue', type=str, help="Create a message queue entry, uses address and queue names from config.")

group = parser.add_mutually_exclusive_group()
group.add_argument('-a','--all', nargs='?', const=True, type=bool, help='Create all single measures, double measures, and lines.')
group.add_argument('-l','--line', nargs='?', const=-1, type=int, help='Create all lines, or create a single line if an index is given.')
group.add_argument('-m','--measure', nargs='?', const=-1, type=int, help='Create all measures, or create a single measure if an index is given.')


args = parser.parse_args()

score = Score(args.path)

out_path = f"{args.path}{os.path.sep}slices{os.path.sep}"
measure_path = f"{out_path}measures{os.path.sep}"
line_path = f"{out_path}lines{os.path.sep}"
double_measure_path = f"{out_path}double_measures{os.path.sep}"

if args.output:
    out_path = output

stored_slices = []
def save_slice(score_slice, path):
    stored_slices.append(score_slice)
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    score_slice.get_image().save(path + score_slice.get_name())

if args.all or args.measure == -1:
    for score_slice in score.get_measure_slices():
        save_slice(score_slice, measure_path)

if args.all:
    for score_slice in score.get_measure_slices(2):
        if score_slice.same_page:
            save_slice(score_slice, double_measure_path)

if args.all or args.line == -1:
    for score_slice in score.get_line_slices():
        save_slice(score_slice, line_path)

if args.measure != None and args.measure >= 0:
    for score_slice in score.get_measure_slices(start=args.measure, end=args.measure+1):
        save_slice(score_slice, measure_path)

if args.line != None and args.line >= 0 and args.line < score.get_line_count():
    for score_slice in score.get_line_slices(start=args.line, end=args.line+1):
        save_slice(score_slice, measure_path)

if args.store_in_db:
    address = cfg.mongodb_address
    client = MongoClient(address.ip, address.port)
    db = client[cfg.db_name]

    res = db[cfg.col_score].insert_one(score.to_db_dict())
    print(f"added entry {res.inserted_id} to scores collection")
    for score_slice in stored_slices:
        res = db[cfg.col_slice].insert_one(score_slice.to_db_dict())
        print(f"added entry {res.inserted_id} to slices collection")

if args.message_queue:
    address = cfg.rabbitmq_address
    connection = pika.BlockingConnection(pika.ConnectionParameters(address.ip, address.port))
    channel = connection.channel()
    channel.queue_declare(queue=cfg.mq_score)
    channel.basic_publish(exchange='', routing_key=cfg.mq_score, body=json.dumps({"name": score.name}))
    print(f"Published processed score {score.name} to message queue!")
    connection.close()