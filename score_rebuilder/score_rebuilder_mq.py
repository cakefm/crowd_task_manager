import sys
import pika
import json

sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
import common.tree_tools as tt
import xml.dom.minidom as xml
from bson.objectid import ObjectId

from pymongo import MongoClient

# Score rebuilder should always re-index, and rely on the measure order instead of the n./label-attribute
def callback(ch, method, properties, body):
    data = json.loads(body)
    sheet_name = data['name']
    task_id = data['task_id']

    client = MongoClient(cfg.mongodb_address.ip, cfg.mongodb_address.port)
    db = client[cfg.db_name]

    # Get MEI file and measures
    mei_path = fsm.get_sheet_whole_directory(sheet_name) / "aligned.mei"
    mei_xml = xml.parse(str(mei_path))
    mei_measures = mei_xml.getElementsByTagName("measure")

    # Obtain corresponding task and slice
    task = db[cfg.col_task].find_one({"_id" : ObjectId(task_id)})
    measure_staff_slice = db[cfg.col_slice].find_one({"_id" : ObjectId(task["slice_id"])})
    slice_measures = mei_measures[measure_staff_slice["start"]: measure_staff_slice["end"]]

    # Get aggregated XML
    aggregated_result = db[cfg.col_aggregated_result].find_one({"task_id" : task_id})
    aggregated_xml = xml.parseString(aggregated_result["xml"]).documentElement
    aggregated_measures = aggregated_xml.getElementsByTagName("measure")

    # Apply the changes

    # Perform all skeletal modifications in stage 0
    if task["stage"]==0:
        # Adjust measure skeleton
        if len(aggregated_measures) > len(slice_measures):
            for measure in aggregated_measures[len(slice_measures):]:
                mei_measures[0].parentNode.insertBefore(measure, slice_measures[-1].nextSibling)

        # TODO: Adjust staff skeleton
        # ...

        # TODO: Adjust facsimile values
        # ...

        # Re-enumerate
        for index, measure in enumerate(mei_measures):
            measure.setAttribute("n", str(index))
            measure.setAttribute("label", str(index))


    # Perform non-skeletal modifications in any other stage
    else:
        for measure_original, measure_agg in zip(slice_measures, aggregated_measures):
            staffs_original = measure_original.getElementsByTagName("staff")[measure_staff_slice["staff_start"]: measure_staff_slice["staff_end"]]
            staffs_aggregated = measure_agg.getElementsByTagName("staff")
            for staff_original, staff_aggregated in zip(staffs_original, staffs_aggregated):
                tt.replace_child_nodes(staff_original, staff_aggregated.childNodes)

    # Write MEI file
    with open(str(mei_path), 'w') as mei_file:
        mei_file.write(tt.purge_non_element_nodes(mei_xml.documentElement).toprettyxml())

    status_update_msg = {
    '_id': task_id,
    'module': 'score_rebuilder',
    'status': 'complete'
    }

    global channel
    channel.queue_declare(queue=cfg.mq_task_scheduler_status)
    channel.basic_publish(exchange="", routing_key=cfg.mq_task_scheduler_status, body=json.dumps(status_update_msg))


address = cfg.rabbitmq_address
connection = pika.BlockingConnection(pika.ConnectionParameters(address.ip, address.port))
channel = connection.channel()
channel.queue_declare(queue=cfg.mq_score_rebuilder)
channel.basic_consume(queue=cfg.mq_score_rebuilder, on_message_callback=callback, auto_ack=True)

print('Score rebuilder is listening...')
channel.start_consuming()