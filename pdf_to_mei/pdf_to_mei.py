import sys
import pika
import json

sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm

import measure_detector.folder_to_mei_imgmd as to_mei

from pymongo import MongoClient
from bson.objectid import ObjectId
from pdf2image import convert_from_path
from pathlib import Path


connection = pika.BlockingConnection(pika.ConnectionParameters(
    cfg.rabbitmq_address.ip,
    cfg.rabbitmq_address.port
    ))
channel = connection.channel()
channel.queue_declare(queue=cfg.mq_new_item)


def add_to_queue(queue, routing_key, msg):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=cfg.rabbitmq_address.ip,
            port=cfg.rabbitmq_address.port))
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange='', routing_key=routing_key, body=msg)
    connection.close()


def callback(ch, method, properties, body):
    # Decode body and obtain pdf id
    data = json.loads(body)
    pdf_id = data['_id']

    # Initiate mongo client and sheet collection
    client = MongoClient(
        cfg.mongodb_address.ip,
        cfg.mongodb_address.port)
    db = client[cfg.db_name]
    sheet_collection = db[cfg.col_sheet]

    # Get PDF sheet entry
    pdf_sheet = sheet_collection.find_one(ObjectId(pdf_id))
    print(pdf_sheet)
    pdf_sheet_path = Path(pdf_sheet["sheet_path"])
    pdf_sheet_name = pdf_sheet_path.stem
    if not pdf_sheet:
        raise Exception(f"PDF Sheet under id {pdf_id} does not exist!")

    # PDF -> JPEG
    print("Converting PDF to JPEG page images...")
    i = 1
    pages = []
    img_pages_path = fsm.get_sheet_pages_directory(pdf_sheet_name)
    while True:
        try:
            page = convert_from_path(pdf_sheet_path.absolute(), 300, first_page=i, last_page=i+1)[0]
            page_path = img_pages_path / f'page_{i}.jpg'
            page.save(page_path, 'JPEG')
            sheet_collection.update_one({'sheet_path': str(pdf_sheet_path)},
                                        {'$push': {'pages_path': str(page_path)}})
            del page
            print(f"{i} pages out of {len(pages)}")
        except:
            print("Reached end of PDF")
            break
        i += 1
    print("PDF conversion finished succesfully!")

    # JPEG -> MEI
    if (fsm.skeleton_exists(pdf_sheet_name)):
        print("Using pre-existing skeleton, skipping measure detection...")
    else:
        print("Converting JPEG pages to MEI skeleton via measure detector...")
        to_mei.run(pdf_sheet_name, connection)

    # Update sheet on mongo
    mei_path = fsm.get_sheet_whole_directory(pdf_sheet_name) / "aligned.mei"
    sheet_collection.update_one({'_id': ObjectId(pdf_id)},
                                {'$push': {'mei_path': str(mei_path)}})

    # Output name to sheet queue
    status_update_msg = {
        '_id': pdf_id,
        'module': 'measure_detector',
        'status': 'complete',
        'name': pdf_sheet_name}
    add_to_queue(
        cfg.mq_omr_planner_status,
        cfg.mq_omr_planner_status,
        json.dumps(status_update_msg))
    print(f"Published PDF->MEI converted sheet {pdf_sheet_name} to message queue!")


def main():
    try:
        print('PDF to MEI converter is listening...')
        while True:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=cfg.rabbitmq_address.ip,
                    port=cfg.rabbitmq_address.port))
            channel = connection.channel()
            method_frame, header_frame, body = channel.basic_get(cfg.mq_new_item)
            if method_frame:
                channel.basic_ack(method_frame.delivery_tag)
                callback(channel, method_frame, '', body)
    except KeyboardInterrupt:
        print('interrupted!')


if __name__ == "__main__":
    main()
