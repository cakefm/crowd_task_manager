import argparse
import pika
import pathlib
import os
import sys
import pprint

sys.path.append("..")
import common.settings as settings

from slicer import Score, Slice
from pymongo import MongoClient


parser = argparse.ArgumentParser(description='Get image slices from .mei music scores, given that the images are in the same folder as the .mei file.')
parser.add_argument('path', type=str, help='Path to the main directory of the score')
parser.add_argument('-o','--output', type=str, help='Output path, if not given it will create a folder called "slices" at the same location as the .mei file.')
parser.add_argument('-s','--store_in_db', action="store_true", help="Store whatever slices are being created in mongo db, uses address and collection names from config.")
parser.add_argument('-q','--message_queue', type=str, help="Create a message queue entry, uses address and queue names from config.")
parser.add_argument('-d', '--debug_data', action="store_true", help="Only print the created data structures to the console for debugging and then exit.")

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

if args.debug_data:
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(score.to_db_dict())
    sys.exit()

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
    address = settings.mongo_address
    client = MongoClient(address[0], int(address[1]))
    db = client.trompa_test

    res = db[settings.score_collection_name].insert_one(score.to_db_dict())
    print(f"added entry {res.inserted_id} to scores collection")
    for score_slice in stored_slices:
        res = db[settings.slice_collection_name].insert_one(score_slice.to_db_dict())
        print(f"added entry {res.inserted_id} to slices collection")

if args.message_queue:
    address = settings.rabbitmq_address
    connection = pika.BlockingConnection(pika.ConnectionParameters(address[0], address[1]))
    channel = connection.channel()
    channel.queue_declare(queue=settings.score_queue_name)
    channel.basic_publish(exchange='', routing_key=settings.score_queue_name, body=json.dumps({"name": score.name}))
    print(f"Published processed score {score.name} to message queue!")
    connection.close()